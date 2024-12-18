from datetime import datetime
from io import BytesIO
import json
from include.helpers.minio import get_minio_client

def _store_values(values):
    kml = json.loads(values)
    client = get_minio_client()
    bucket_name = 'chargy'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    chargy_status = kml['kml']['Document']['Placemark']
    data = json.dumps(chargy_status, ensure_ascii=False).encode('utf8')
    file_name = f'{datetime.today().strftime('%Y-%m-%d')}/{datetime.today().strftime('%H:%M:%S')}.json'
    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=file_name,
        data=BytesIO(data),
        length=len(data)
        )
    return f'{objw.bucket_name}/{file_name}'
