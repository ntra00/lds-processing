import argparse
import os

parser = argparse.ArgumentParser(description = 'NDMSO File Mover.')
parser.add_argument('-c', '--config', default=os.path.dirname(__file__) + "/../config/config.yaml", help='Configuration file path.')
parser.add_argument('-j', '--job', default="", help="Job - Root item in config.")
parser.add_argument('-s', '--since', default="", help='SINCE=YYYY-MM-DD|vNN.iNN')
parser.add_argument('-f', '--file', default="", help='File - Specific file to upload/download (only works for bulkdownload_s3_upload job presently).')
parser.add_argument('-l', '--list', action='store_true', help="List Jobs.")

args = parser.parse_args()
