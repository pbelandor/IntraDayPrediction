import time
from azure.storage.file import FileService
from azure.storage.file import ContentSettings
import os, sys

file_service = FileService(account_name='intradayrankingstorage', account_key='mlO4njhHbmewcNaTJ1VXc1Xeal5pcUKqOH7aamr9J7BegqlUZhIoxu7JSiy1By7O3y3tvzUdkxd16KooSz3FPA==')


generator = file_service.list_directories_and_files('historydata5movingavg/datafrom2012')
for file_or_dir in generator:
	file_service.delete_file("historydata5movingavg", "datafrom2012", file_or_dir.name, timeout=None)
	print("Deleted: "+file_or_dir.name)