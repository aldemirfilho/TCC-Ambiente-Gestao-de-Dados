import os
class SendMinio:

    def __init__(self, client,bucket_name, object_name, file_path):
        self.__client = client
        self.__bucket_name = bucket_name
        self.__object_name = object_name
        self.__file_path = file_path

    def __send2minio(self):
        self.__client.fput_object(
                bucket_name=self.__bucket_name,
                object_name=self.__object_name,
                file_path=self.__file_path
            )

    def send2bucket(self):
        client = self.__client
        if client.bucket_exists(self.__bucket_name):
            print("Bucket existente... enviando arquivos")
            self.__send2minio()
        else:
            print("Criando bucket...")
            client.make_bucket(self.__bucket_name)
            print("Enviando arquivos...")
            self.__send2minio()
            
    def sendParquet2bucket(self):
        client = self.__client
        root_local = self.__file_path
        root_remote = self.__object_name
        for root, _, files in os.walk(self.__file_path):
            for file in files:
                local_file_path = os.path.join(root_local, file)
                remote_object_name = os.path.join(root_remote, file)
                
                with open(local_file_path, "rb") as file_data:
                    print(remote_object_name)
                    self.__file_path = local_file_path
                    self.__object_name = remote_object_name
                    self.send2bucket() 
        self.__file_path = root_local
        self.__object_name = root_remote   
    
    def findFilesInBucket(self, prefix):
        result = []
        if self.__client.bucket_exists(self.__bucket_name):
            found_items =  self.__client.list_objects(self.__bucket_name,  recursive=True, prefix=prefix)
            for file in found_items:
                result.append(file)
        return result

    def removeFiles(self, path):
        objects_remove = self.findFilesInBucket(path)
        for obj in objects_remove:
            self.__client.remove_object(self.__bucket_name, obj.object_name)

    def downloadFile(self, bucket_file_path, destination_path):
        self.__client.fget_object(
            bucket_name=self.__bucket_name,
            object_name=bucket_file_path,
            file_path=destination_path
        )

    def removeFiles(self, path):
        objects_remove = self.findFilesInBucket(path)
        for obj in objects_remove:
            self.__client.remove_object(self.__bucket_name, obj.object_name)