import logging
import os
import subprocess

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def download_maven_jar(group_id, artifact_id, version, output_dir="."):
    base_url = "https://repo1.maven.org/maven2"

    group_path = group_id.replace(".", "/")
    jar_name = f"{artifact_id}-{version}.jar"
    jar_url = f"{base_url}/{group_path}/{artifact_id}/{version}/{jar_name}"

    output_path = os.path.join(output_dir, jar_name)

    print(f"Downloading {jar_url} ...")
    response = requests.get(jar_url)

    if response.status_code == 200:
        with open(output_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded to: {output_path}")
    else:
        print(f"Failed to download. Status code: {response.status_code}")


def xml_to_xsd_with_trang(xml_file, xsd_file, output_dir, group_id, artifact_id, version):
    download_maven_jar(
        group_id=group_id,
        artifact_id=artifact_id,
        version=version,
        output_dir=output_dir,
    )

    trang_jar = f"{artifact_id}-{version}.jar"
    trang_jar_file = os.path.join(output_dir, trang_jar)
    if not os.path.isfile(xml_file):
        logging.error(f"Input XML file not found: {xml_file}")
        return
    if not os.path.isfile(trang_jar_file):
        logging.error(f"Trang JAR not found: {trang_jar_file}")
        return

    os.makedirs(os.path.dirname(xsd_file), exist_ok=True)

    try:
        cmd = ["java", "-jar", trang_jar_file, xml_file, xsd_file]
        subprocess.run(cmd, check=True)
        logging.info(f"XSD generated at: {xsd_file}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Trang failed: {e}")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", ".")
    xml_path = os.path.join(data_home, "file_data", "xml", "notes.xml")
    xsd_path = os.path.join(data_home, "file_data", "xml", "notes.xsd")
    jar_path = os.path.join(data_home, "libs")
    group_id = "com.thaiopensource"
    artifact_id = "trang"
    version = "20091111"

    xml_to_xsd_with_trang(
        xml_file=xml_path,
        xsd_file=xsd_path,
        output_dir=jar_path,
        group_id=group_id,
        artifact_id=artifact_id,
        version=version,
    )
