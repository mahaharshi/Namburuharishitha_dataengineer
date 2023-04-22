import logging
from typing import List
import boto3
import csv
import xml.etree.ElementTree as Xet

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Function to write fields into a CSV file
def write_into_csv(fields: List[str]) -> None:
    with open("output.csv", 'a', encoding="UTF-8") as csvfile: 
        csvwriter = csv.writer(csvfile, lineterminator='\n') 
        csvwriter.writerow(fields) 

# Function to upload a CSV file to Amazon S3 bucket
def upload_to_amazon_s3_bucket(file_path: str, bucket_name: str, object_name: str) -> bool:
    try:
        s3 = boto3.client('s3')
        response = s3.upload_file(file_path, bucket_name, object_name)
    except Exception as e:
        logger.error(f"Error uploading file to S3 bucket: {e}")
        return False
    return True

# Main function
def main():
    try:
        # Open the XML file for reading
        xml_file = "DLTINS_20210117_01of01.xml"
        f = open(xml_file, "rb")

        # Define the columns for the CSV file
        cols = ["Id", "FullNm", "ClssfctnTp", "CmmdtyDerivInd", "NtnlCcy"]

        # Write the column names to the CSV file
        write_into_csv(cols)

        # Parse the XML file
        namespace = "{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}"
        xmlparse = Xet.parse(f)
        root = xmlparse.getroot()
        temp = []

        # Find all the "FinInstrmGnlAttrbts" elements in the XML file
        for iterator in root.iter():
            if(iterator.tag == namespace+"FinInstrmGnlAttrbts"):
                temp.append(iterator)

        # Extract the data from each "FinInstrmGnlAttrbts" element and write it to the CSV file
        for iterator in temp:
            id = iterator.find(namespace+"Id")
            FullNm = iterator.find(namespace+"FullNm")
            ClssfctnTp = iterator.find(namespace+"ClssfctnTp")
            CmmdtyDerivInd = iterator.find(namespace+"CmmdtyDerivInd")
            NtnlCcy = iterator.find(namespace+"NtnlCcy")

            # Convert None values to empty strings
            if(id != None):
                id = id.text
            if(FullNm != None):
                FullNm = FullNm.text
            if(ClssfctnTp != None):
                ClssfctnTp = ClssfctnTp.text
            if(CmmdtyDerivInd != None):
                CmmdtyDerivInd = CmmdtyDerivInd.text
            if(NtnlCcy != None):
                NtnlCcy = NtnlCcy.text

            # Write the data to the CSV file
            if(id != None):
                write_into_csv([id, FullNm, ClssfctnTp, CmmdtyDerivInd, NtnlCcy])

        # Upload the CSV file to S3 bucket
        bucket_name = "assignment-s3"
        object_name = 'output.csv'
        if upload_to_amazon_s3_bucket('output.csv', bucket_name, object_name):
            logger.info(f"File {object_name} uploaded successfully to S3 bucket {bucket_name}")
        else:
           logger.error("Failed to upload csv to s3")
    except Exception as e:
        logger.error(f"Error in main: {e}")
