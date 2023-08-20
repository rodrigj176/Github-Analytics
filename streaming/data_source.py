from datetime import datetime
import socket
import time
import requests
import os
import re

token = os.getenv('TOKEN')
url = 'https://api.github.com/search/repositories?q=+language:Python+language:Ruby+language:PHP&sort=updated&order=desc'
auth = "token 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"
visited = []

TCP_IP = "0.0.0.0"
TCP_PORT = 9999
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(15)
print("Waiting for TCP connection...")
# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting sending data.")

while True:
    try:
        file = requests.get(url, headers={"Authorization": auth})

        cont = (file.json())

        data = cont.get("items")
            
        for repo in data:
            
            id = repo["full_name"]
            push_time = repo["pushed_at"]
            scount = repo['stargazers_count']
            
            description = repo['description']
            language = repo['language']
            if description == None:
                description = ''
            if language == None:
                language = ''
            if scount == None:
                scount =''
            
            description_string = ''
            real_list = []
            description_list = re.sub('[^a-zA-Z ]', '|||', description)
            description_list = description_list.replace(" ","|||")
            description_list = description_list.split('|||')
            for things in description_list:
                if things != '':
                    real_list.append(things)

            for words in real_list:
                if not description_string:
                    description_string = description_string + words
                else:
                    description_string = description_string + '-' + words
            
    
            push_time = push_time.replace("T","|")
            push_time = push_time.replace("Z","")
           
            if id in visited:
                print("Im skipping:",id)
                time.sleep(0.5)
                continue

            data = (f"{scount}" + ' ' + language + ' ' + push_time + ' ' + description_string +'\n').encode()
            
            conn.send(data)
            print('Data being sent to spark_app through stream with format --- star count, PL, pushed_at, description):')
            print(data)
            visited.append(id)
           
            time.sleep(0.5)
   

    except KeyboardInterrupt:
            s.shutdown(socket.SHUT_RD)