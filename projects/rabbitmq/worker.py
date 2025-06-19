import json
import os
from glob import glob
from typing import Tuple, Union, Optional
import base64
import shutil
import tempfile
import pika
from loguru import logger

from mineru.data.data_reader_writer import FileBasedDataWriter
from mineru.data.data_reader_writer.s3 import S3DataReader, S3DataWriter
from mineru.utils.config_reader import get_bucket_name, get_s3_config
from mineru.cli.common import convert_pdf_bytes_to_bytes_by_pypdfium2
from mineru.backend.pipeline.pipeline_analyze import doc_analyze as pipeline_doc_analyze
from mineru.backend.pipeline.model_json_to_middle_json import result_to_middle_json as pipeline_result_to_middle_json
from mineru.backend.pipeline.pipeline_middle_json_mkcontent import union_make as pipeline_union_make
from mineru.utils.enum_class import MakeMode
from mineru.utils.draw_bbox import draw_layout_bbox, draw_span_bbox


pdf_extensions = [".pdf"]
office_extensions = [".ppt", ".pptx", ".doc", ".docx"]
image_extensions = [".png", ".jpg", ".jpeg"]

def init_writers(
    file_path: Optional[str] = None,
    output_path: Optional[str] = None,
    output_image_path: Optional[str] = None,
) -> Tuple[
    Union[S3DataWriter, FileBasedDataWriter],
    Union[S3DataWriter, FileBasedDataWriter],
    bytes,
    str,
]:
    """
    Initialize writers based on path type

    Args:
        file_path: file path (local path or S3 path)
        output_path: Output directory path
        output_image_path: Image output directory path

    Returns:
        Tuple[writer, image_writer, file_bytes, file_extension]: Returns initialized writer tuple and file content
    """
    file_extension: str = ""
    file_bytes: bytes = b""
    
    output_path_checked = output_path if output_path else "output"
    output_image_path_checked = output_image_path if output_image_path else f"{output_path_checked}/images"


    if file_path:
        is_s3_path = file_path.startswith("s3://")
        if is_s3_path:
            bucket = get_bucket_name(file_path)
            ak, sk, endpoint = get_s3_config(bucket)

            writer = S3DataWriter(
                output_path_checked, bucket=bucket, ak=ak, sk=sk, endpoint_url=endpoint
            )
            image_writer = S3DataWriter(
                output_image_path_checked, bucket=bucket, ak=ak, sk=sk, endpoint_url=endpoint
            )
            # 临时创建reader读取文件内容
            temp_reader = S3DataReader(
                "", bucket=bucket, ak=ak, sk=sk, endpoint_url=endpoint
            )
            file_bytes = temp_reader.read(file_path)
            file_extension = os.path.splitext(file_path)[1]
        else:
            writer = FileBasedDataWriter(output_path_checked)
            image_writer = FileBasedDataWriter(output_image_path_checked)
            os.makedirs(output_image_path_checked, exist_ok=True)
            with open(file_path, "rb") as f:
                file_bytes = f.read()
            file_extension = os.path.splitext(file_path)[1]
    else:
        raise ValueError("Must provide file_path")


    return writer, image_writer, file_bytes, file_extension


def process_file_mineru(
    file_bytes: bytes,
    file_extension: str,
    image_writer: Union[S3DataWriter, FileBasedDataWriter],
    parse_method: str = "auto",
    lang: str = "ch",
    formula_enable: bool = True,
    table_enable: bool = True,
):
    processed_bytes = file_bytes
    if file_extension in pdf_extensions:
        processed_bytes = convert_pdf_bytes_to_bytes_by_pypdfium2(file_bytes, 0, None)
    
    infer_results, all_image_lists, all_pdf_docs, lang_list, ocr_enabled_list = pipeline_doc_analyze(
        [processed_bytes], [lang], parse_method, formula_enable, table_enable
    )
    
    model_list = infer_results[0]
    images_list = all_image_lists[0]
    pdf_doc = all_pdf_docs[0]
    _lang = lang_list[0]
    _ocr_enable = ocr_enabled_list[0]

    model_json = json.loads(json.dumps(model_list)) # deepcopy
    
    middle_json = pipeline_result_to_middle_json(model_list, images_list, pdf_doc, image_writer, _lang, _ocr_enable, formula_enable)
    
    md_content = pipeline_union_make(middle_json["pdf_info"], MakeMode.MM_MD, "images")
    content_list = pipeline_union_make(middle_json["pdf_info"], MakeMode.CONTENT_LIST, "images")
    
    return model_json, middle_json, content_list, md_content, processed_bytes, middle_json["pdf_info"]


def handle_message(ch, method, properties, body):
    temp_dir = tempfile.mkdtemp()
    try:
        logger.info("Received a new message")
        message = json.loads(body)
        file_name = message.get("file_name")
        file_content_base64 = message.get("file_content_base64")
        correlation_id = properties.correlation_id

        if not file_name or not file_content_base64:
            raise ValueError("`file_name` and `file_content_base64` are required")
        
        logger.info(f"Processing file: {file_name}")

        file_path = os.path.join(temp_dir, file_name)
        with open(file_path, "wb") as f:
            f.write(base64.b64decode(file_content_base64))

        parse_method = message.get("parse_method", "auto")
        lang = message.get("lang", "ch")
        formula_enable = message.get("formula_enable", True)
        table_enable = message.get("table_enable", True)

        output_path = os.path.join(temp_dir, os.path.splitext(file_name)[0])
        output_image_path = os.path.join(output_path, "images")

        _, image_writer, file_bytes, file_extension = init_writers(
            file_path=file_path,
            output_path=output_path,
            output_image_path=output_image_path,
        )

        (
            model_json,
            middle_json,
            content_list,
            md_content,
            processed_bytes,
            pdf_info,
        ) = process_file_mineru(
            file_bytes,
            file_extension,
            image_writer,
            parse_method,
            lang,
            formula_enable,
            table_enable,
        )

        result_queue = os.getenv("RABBITMQ_RESULT_QUEUE", "mineru_results_queue")
        result_message = {
            "file_name": file_name,
            "md": md_content,
            "content_list": content_list,
            "middle_json": middle_json,
        }

        # Add correlation_id to result message if it was provided
        if correlation_id:
            result_message["correlation_id"] = correlation_id

        logger.info(f"Publishing result for correlation_id: {correlation_id}")

        ch.queue_declare(queue=result_queue, durable=True)
        ch.basic_publish(
            exchange="",
            routing_key=result_queue,
            body=json.dumps(result_message, ensure_ascii=False),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                correlation_id=correlation_id,  # Set correlation_id in message properties
            ),
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"Message processed and acknowledged for file: {file_name}")

    except Exception as e:
        logger.error(f"Failed to process message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    finally:
        shutil.rmtree(temp_dir)
        logger.info("Cleaned up temporary directory")


def main():
    host = os.getenv("RABBITMQ_HOST", "localhost")
    queue = os.getenv("RABBITMQ_QUEUE", "mineru_parse_queue")

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue, on_message_callback=handle_message)

    logger.info(f" [*] Waiting for messages in '{queue}'. To exit press CTRL+C")
    try:
        channel.start_consuming()
    except (KeyboardInterrupt, SystemExit):
        channel.stop_consuming()
        logger.info(" [*] Exiting...")
    finally:
        connection.close()
        logger.info("RabbitMQ connection closed.")


if __name__ == "__main__":
    # os.environ['MINERU_MODEL_SOURCE'] = "modelscope"
    main() 