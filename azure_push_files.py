import time
from azure.storage.file import FileService
from azure.storage.file import ContentSettings
import os, sys

start_time = time.time()

file_service = FileService(account_name='intradayrankingstorage', account_key='mlO4njhHbmewcNaTJ1VXc1Xeal5pcUKqOH7aamr9J7BegqlUZhIoxu7JSiy1By7O3y3tvzUdkxd16KooSz3FPA==')
data_folder = "datafrom2012"
path_to_dir = os.path.join(os.getcwd(), data_folder)

def pushFiles():
	print("Uploading files to File Share historydata5movingavg...")
	generator = file_service.list_directories_and_files('historydata5movingavg/datafrom2012')
	try:
		files = os.listdir(path_to_dir)
	except:
		print("Folder "+data_folder+" does not exist")
		sys.exit()

	for file in files:
		try:
			file_service.create_file_from_path('historydata5movingavg',
				'datafrom2012', 
				file, 
				file, 
				content_settings = ContentSettings(content_type='text/csv') 
			)
		except:
			print("Error Uploading file: "+ file)
			continue
		print("Uploaded/Updated: "+file)

os.getcwd()
os.chdir(data_folder)
[os.rename(f, f.replace('.csv', '')) for f in os.listdir('.') if not f.startswith('.')]
pushFiles()
print("---Execution Time: %s seconds ---" % (time.time() - start_time))

