#!/bin/bash

# Функция логирования
function log() {
    sep="----------------------------------------------------------"
    echo "[$(date)] $sep "
    echo "[$(date)] [INFO] $1"
}

# Проверяем, передан ли аргумент (имя файла)
if [ -n "$1" ]; then
    FILE_NAME="$1"
    log "File name provided: $FILE_NAME"
else
    log "No file name provided, copying all files"
fi

# Копируем данные из S3 в зависимости от того, передано ли имя файла
if [ -n "$FILE_NAME" ]; then
    # Копируем конкретный файл
    log "Copying specific file from HDFS to S3"
    hadoop distcp /user/ubuntu/data/$FILE_NAME s3a://{{ s3_bucket }}/$FILE_NAME

else
    # Копируем все данные
    log "Copying all data from HDFS to S3"
    hadoop distcp /user/ubuntu/data s3a://{{ s3_bucket }}/
fi

# Выводим содержимое директории для проверки
log "Listing files in HDFS directory"
#hdfs dfs -ls /user/ubuntu/data
s3cmd --config=/home/ubuntu/.s3cfg ls s3a://{{ s3_bucket }}

# Проверяем успешность выполнения операции
if [ $? -eq 0 ]; then
    log "Data was successfully copied to S3"
else
    log "Failed to copy data to S3"
    exit 1
fi
