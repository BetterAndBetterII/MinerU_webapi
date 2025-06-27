import json
import os
from base64 import b64encode
from glob import glob
from typing import Tuple, Union, Optional

import uvicorn
from fastapi import FastAPI, UploadFile
from fastapi.responses import JSONResponse
from loguru import logger

from mineru.data.data_reader_writer import FileBasedDataWriter
from mineru.data.data_reader_writer.s3 import S3DataReader, S3DataWriter
from mineru.utils.config_reader import get_bucket_name, get_s3_config
from fastapi import Form
from mineru.cli.common import convert_pdf_bytes_to_bytes_by_pypdfium2
from mineru.backend.vlm.vlm_analyze import doc_analyze as vlm_doc_analyze
from mineru.backend.vlm.vlm_middle_json_mkcontent import union_make as vlm_union_make
from mineru.utils.enum_class import MakeMode
from mineru.utils.draw_bbox import draw_layout_bbox, draw_span_bbox

app = FastAPI()

pdf_extensions = [".pdf"]
office_extensions = [".ppt", ".pptx", ".doc", ".docx"]
image_extensions = [".png", ".jpg", ".jpeg"]

def init_writers(
    file_path: Optional[str] = None,
    file: Optional[UploadFile] = None,
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
        file: Uploaded file object
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
    elif file:
        # 处理上传的文件
        content = file.file.read()
        if isinstance(content, str):
            file_bytes = content.encode("utf-8")
        else:
            file_bytes = content
        file_extension = os.path.splitext(file.filename)[1] if file.filename else ""

        writer = FileBasedDataWriter(output_path_checked)
        image_writer = FileBasedDataWriter(output_image_path_checked)
        os.makedirs(output_image_path_checked, exist_ok=True)
    else:
        raise ValueError("Must provide either file or file_path")


    return writer, image_writer, file_bytes, file_extension


def process_file_mineru(
    file_bytes: bytes,
    file_extension: str,
    image_writer: Union[S3DataWriter, FileBasedDataWriter],
    backend: str = "transformers",
    server_url: Optional[str] = None,
):
    processed_bytes = file_bytes
    if file_extension in pdf_extensions:
        processed_bytes = convert_pdf_bytes_to_bytes_by_pypdfium2(file_bytes, 0, None)

    middle_json, infer_result = vlm_doc_analyze(
        processed_bytes, image_writer=image_writer, backend=backend, server_url=server_url
    )

    pdf_info = middle_json["pdf_info"]
    md_content = vlm_union_make(pdf_info, MakeMode.MM_MD, "images")
    content_list = vlm_union_make(pdf_info, MakeMode.CONTENT_LIST, "images")

    model_output = ("\n" + "-" * 50 + "\n").join(infer_result)

    return middle_json, content_list, md_content, processed_bytes, pdf_info, model_output


def encode_image(image_path: str) -> str:
    """Encode image using base64"""
    with open(image_path, "rb") as f:
        return b64encode(f.read()).decode()


@app.post(
    "/file_parse",
    tags=["projects"],
    summary="Parse files (supports local files and S3)",
)
async def file_parse(
    file: Optional[UploadFile] = None,
    file_path: Optional[str] = Form(None),
    backend: str = Form("transformers"),
    server_url: Optional[str] = Form(None),
    is_json_md_dump: bool = Form(False),
    output_dir: str = Form("output"),
    return_layout: bool = Form(False),
    return_info: bool = Form(False),
    return_content_list: bool = Form(False),
    return_images: bool = Form(False),
):
    """
    Execute the process of converting PDF to JSON and MD, outputting MD and JSON files
    to the specified directory.

    Args:
        file: The PDF file to be parsed. Must not be specified together with
            `file_path`
        file_path: The path to the PDF file to be parsed. Must not be specified together
            with `file`
        backend: The backend for parsing pdf. Default is `transformers`.
            `vlm-transformers`: More general.
            `vlm-sglang-engine`: Faster(engine).
            `vlm-sglang-client`: Faster(client).
        server_url: When the backend is `sglang-client`, you need to specify the server_url, for example:`http://127.0.0.1:30000`
        is_json_md_dump: Whether to write parsed data to .json and .md files. Default
            to False. Different stages of data will be written to different .json files
            (3 in total), md content will be saved to .md file
        output_dir: Output directory for results. A folder named after the PDF file
            will be created to store all results
        return_layout: Whether to return parsed PDF layout. Default to False
        return_info: Whether to return parsed PDF info. Default to False
        return_content_list: Whether to return parsed PDF content list. Default to False
    """
    try:
        if (file is None and file_path is None) or (
            file is not None and file_path is not None
        ):
            return JSONResponse(
                content={"error": "Must provide either file or file_path"},
                status_code=400,
            )

        # Get PDF filename
        if file_path:
            file_name = os.path.basename(file_path)
        elif file and file.filename:
            file_name = os.path.basename(file.filename)
        else:
             return JSONResponse(
                content={"error": "Could not determine filename."},
                status_code=400,
            )
        
        file_name = file_name.split(".")[0]
        output_path = f"{output_dir}/{file_name}"
        output_image_path = f"{output_path}/images"

        # Initialize readers/writers and get PDF content
        writer, image_writer, file_bytes, file_extension = init_writers(
            file_path=file_path,
            file=file,
            output_path=output_path,
            output_image_path=output_image_path,
        )

        if file_extension not in pdf_extensions + image_extensions:
            return JSONResponse(
                content={"error": f"File type {file_extension} is not supported."},
                status_code=400,
            )

        backend_name = backend[4:] if backend.startswith("vlm-") else backend

        # Process PDF
        middle_json, content_list, md_content, processed_bytes, pdf_info, model_output = process_file_mineru(
            file_bytes, file_extension, image_writer, backend_name, server_url
        )

        # If results need to be saved
        if is_json_md_dump:
            writer.write_string(
                f"{file_name}_content_list.json", json.dumps(content_list, indent=4, ensure_ascii=False)
            )
            writer.write_string(f"{file_name}.md", str(md_content))
            writer.write_string(
                f"{file_name}_middle.json", json.dumps(middle_json, indent=4, ensure_ascii=False)
            )
            writer.write_string(
                f"{file_name}_model_output.txt",
                model_output,
            )
            # Save visualization results
            if not isinstance(writer, S3DataWriter):
                draw_layout_bbox(pdf_info, processed_bytes, output_path, f"{file_name}_layout.pdf")
                draw_span_bbox(pdf_info, processed_bytes, output_path, f"{file_name}_span.pdf")

        # Build return data
        data = {}
        if return_layout:
            data["layout"] = middle_json
        if return_info:
            data["info"] = middle_json
        if return_content_list:
            data["content_list"] = content_list
        if return_images:
            if not isinstance(image_writer, S3DataWriter):
                image_paths = glob(f"{output_image_path}/*.jpg")
                data["images"] = {
                    os.path.basename(
                        image_path
                    ): f"data:image/jpeg;base64,{encode_image(image_path)}"
                    for image_path in image_paths
                }
            else:
                logger.warning("return_images is not supported for S3 storage yet.")
                data["images"] = {}
                
        data["md_content"] = md_content  # md_content is always returned

        return JSONResponse(data, status_code=200)

    except Exception as e:
        logger.exception(e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


if __name__ == "__main__":
    # os.environ['MINERU_MODEL_SOURCE'] = "modelscope"
    uvicorn.run(app, host="0.0.0.0", port=8888)
