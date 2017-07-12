import time
from azure.storage.file import FileService
from azure.storage.file import ContentSettings
import os, sys

start_time = time.time()

file_service = FileService(account_name='intradayrankingstorage', account_key='mlO4njhHbmewcNaTJ1VXc1Xeal5pcUKqOH7aamr9J7BegqlUZhIoxu7JSiy1By7O3y3tvzUdkxd16KooSz3FPA==')
data_folder = "datafrom2012"
path_to_dir = os.path.join(os.getcwd(), data_folder)


def pullFiles():
	print("Files Not there, Pulling data from Azure...")
	generator = file_service.list_directories_and_files('historydata5movingavg/datafrom2012')
	for file_or_dir in generator:
		try:
			file_service.get_file_to_path('historydata5movingavg', None, 'datafrom2012/'+file_or_dir.name, data_folder+'/'+file_or_dir.name+'.csv')
		except:
			print("Error downloading file: "+ file_or_dir.name)
			continue
		print("Downloaded: "+file_or_dir.name)



if (os.path.isdir(path_to_dir)):
	print("Yes, directory exists")
	if os.listdir(path_to_dir)==[]:
		print("Yes, is empty")
		pullFiles()
	else:
		print("Has files.")
		print(os.listdir(path_to_dir))
	sys.exit()

else:
	print("Folder unavailable. \nCreating folder "+data_folder+" and Pulling data from Azure...")
	os.getcwd()
	os.mkdir(data_folder)
	pullFiles()




print("---Execution Time: %s seconds ---" % (time.time() - start_time))