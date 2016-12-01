import socket, json

obj_frame = """{
	"request": "add_video_to_queue",
	"video_file": "test_video2.mp4"
}"""

s = socket.socket()
s.connect(("",7331))

s.send(obj_frame)
respond = json.loads(s.recv(512))
s.close()
print respond['respond']