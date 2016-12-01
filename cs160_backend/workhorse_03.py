import requests, json, os, sys, Queue, sqlite3, time, threading, ffmpy, shutil, socket
from threading import Thread, Lock
from requests.exceptions import ConnectionError, ReadTimeout
from ffprobe import FFProbe

SIZE = 512

mutex = Lock()
# mutex.release()
extension = 'png'
api_servers_config_file = "servers.json"

global sound_of_silence, debug, clean_up, use_jpeg
sound_of_silence=True 
debug=False
clean_up = False
use_jpeg = False


class api_server:
   def __init__(self, name, url, status=False):
      #self.status = status
      self.online = status
      self.available = status
      self.name = name
      self.url = url

   def check_server_status(self):
      er = False
      try:
         p = requests.post(self.url, data={'action':"check_if_ready_to_work"}, timeout=1)
         #self.status = ( p.text == 'answer is False')
         self.online = not ( p == None)
         self.available = ( p.text == 'answer is False')
      except (ConnectionError, ReadTimeout):
         #self.status = False
         self.online = False
         self.available = False

   def print_status(self):
      print("Server: {:20s}  Status: {:7s}   Available: {}".format(self.name, "Online" if self.online else "Offline",self.available))

   def send_i_f_get_points(self, file_name):
      mutex.acquire()
      # self.status = False
      self.available = False
      mutex.release()
      file = {'img': open(file_name, 'rb')}
      p = requests.post(self.url, data={'action':"get_point_from_image_serial"} , files=file)
      points = json.loads(p.text)
      mutex.acquire()
      # self.status = True
      self.available = True
      mutex.release()
      return points

   def send_i_f_and_p_get_i_f(self, file_name, points):
      mutex.acquire()
      # self.status = False
      self.available = False
      mutex.release()
      file = {'img': open(file_name, 'rb')}
      data_to_send = {'action':"draw_ponts_on_image", 'points':json.dumps(points)}
      dfile = requests.post(self.url, data=data_to_send , files=file)
      mutex.acquire()
      # self.status = True
      self.available = True
      mutex.release()
      return dfile.content


def timed_function(func):
   def func_wrapper(*args, **kwargs):
      start_time = time.time()
      process = func(*args, **kwargs)
      end_time = time.time()
      if not sound_of_silence:
         print "total time of {}: {}".format( func.__name__, end_time - start_time) 
      return process
   return func_wrapper

def save_file(file_name, file_content):
   with open(file_name, 'wb') as f:
      f.write(file_content)

def create_db(db_name):
   conn = sqlite3.connect(db_name)
   c = conn.cursor()
   c.execute('''CREATE TABLE points (file_name text, points_json text)''')
   conn.commit()
   conn.close()

def save_points_to_db(db_name, file_name, faces_points):
   file_name_we = get_filename_we(file_name)
   conn = sqlite3.connect(db_name)
   c = conn.cursor()
   c.execute("INSERT INTO points VALUES (?,?)", (file_name_we, json.dumps(faces_points) ) )
   conn.commit()
   conn.close()

def get_points_from_db(db_name, file_name):
   file_name_we = get_filename_we(file_name)
   conn = sqlite3.connect(db_name)
   c = conn.cursor()
   c.execute('SELECT points_json FROM points WHERE file_name=?', (file_name_we,))
   result = c.fetchone()[0]
   conn.commit()
   conn.close()
   return json.loads(result)

def get_filename_we(file_name):
   return os.path.splitext(file_name)[0]

@timed_function
def phase_one(servers, queue, folder_with_frames_name, db_name):
   threads = []
   while not queue.empty():
      for server in servers:
         mutex.acquire()
         # if server.status:
         if server.online and server.available:
            #server.status=False
            server.available=False
            file_name = queue.get()
            mutex.release()
            t = threading.Thread(target=single_phase_one, args=(server, file_name, folder_with_frames_name,db_name, ))
            threads.append(t)
            t.start()
         else:
            mutex.release()
   for t in threads:
      t.join()

@timed_function
def phase_two(servers, queue, folder_with_frames_name, output_folder_name, db_name):
   threads = []
   while not queue.empty():
      for server in servers:
         mutex.acquire()
         # if server.status:
         if server.online and server.available:
            #server.status=False
            server.available=False
            file_name = queue.get()
            mutex.release()
            t = threading.Thread(target=single_phase_two, args=(server, file_name, folder_with_frames_name, output_folder_name, db_name, ))
            threads.append(t)
            t.start()
         else:
            mutex.release()
   for t in threads:
      t.join()


def single_phase_one(server, file_name, folder_with_frames_name, db_name):
   path_to_source_file = "{}/{}".format(folder_with_frames_name, file_name)
   if not sound_of_silence:
      print("phase 1: sending file {} to {}".format(file_name, server.name))
   points = server.send_i_f_get_points(path_to_source_file)
   save_points_to_db(db_name, file_name, points)

def single_phase_two(server, file_name, folder_with_frames_name, output_folder_name, db_name):
   path_to_source_file = "{}/{}".format(folder_with_frames_name, file_name)
   if not sound_of_silence:
      print("phase 2: sending file {} to {}".format(file_name, server.name))
   points_from_db = get_points_from_db(db_name, file_name)
   return_file_content = server.send_i_f_and_p_get_i_f(path_to_source_file, points_from_db)
   save_file("{}/{}".format(output_folder_name, file_name), return_file_content)

def frames_finder_queue_returner(folder_with_frames_name, extension):
   files_first_queue = Queue.Queue()
   files_second_queue = Queue.Queue()
   file_count = 0

   # print("found files: ")
   for file in os.listdir(folder_with_frames_name):
      if file.endswith(".{}".format(extension)):
         files_first_queue.put("{}".format(file))
         files_second_queue.put("{}".format(file))
         file_count+=1
   # print("files found: {}".format(file_count))
   return files_first_queue, files_second_queue, file_count

def create_output_frames_folder(input_frames_folder):
   output_folder_name = "output_{}".format(input_frames_folder)
   create_folder(output_folder_name)
   return output_folder_name

def create_folder(folder_name):
   if not os.path.exists(folder_name):
      os.makedirs(folder_name)

def create_database(db_name):
   if not os.path.exists(db_name):
      print("creating database: {}".format(db_name))
      create_db(db_name)
   return db_name

# def create_database(folder_with_frames_name):
#    db_name = "{}_db".format(folder_with_frames_name)
#    if not os.path.exists(db_name):
#       print("creating database: {}".format(db_name))
#       create_db(db_name)
#    return db_name

def check_servers(servers):
   for server in servers:
      server.check_server_status()
   if not sound_of_silence:
      print("check status")
      for server in servers:
         server.print_status()

def create_servers_from_json(json_file):
   json_data=open(json_file).read()
   data = json.loads(json_data)
   servers = []
   for server in data["servers"]:
      servers.append(api_server(server["Name"], server["Host"]))
      if not sound_of_silence:
         print server["Name"] + " " + server["Host"]
   return servers

def work_with_frames(folder_with_frames_name,
      extension,
      api_servers_config_file, 
      output_folder_name, 
      db_name,
      jpeg_folder=None):
   # if not sound_of_silence:
   #    print ("starting working with {}".format(folder_with_frames_name))

   # Get servers ready
   try:
      servers = create_servers_from_json(api_servers_config_file)
   except IOError:
      print("no server config file found")
      sys.exit()
   # Check if servers online and available
   check_servers(servers)
   
   #find input frames
   queue_one, queue_two, file_count = frames_finder_queue_returner(folder_with_frames_name, extension)
   if not sound_of_silence:
      print("frames found: {}".format(file_count))


   #PHASE 1 - GET POINTS
   if jpeg_folder:
      queue_one, _, _ = frames_finder_queue_returner(jpeg_folder, "jpeg")
      phase_one(servers, queue_one, jpeg_folder, db_name)
   else:
      phase_one(servers, queue_one, folder_with_frames_name, db_name)

   #PHASE 2 - DRAW TRIANGLES
   phase_two(servers, queue_two, folder_with_frames_name,output_folder_name, db_name)

   # if not sound_of_silence:
   #    print ("ending working with {}".format(folder_with_frames_name))

def cut_video_on_frames(input_video_file_name, folder_to_put_frames):
   video_file_name = input_video_file_name 
   metadata=FFProbe(video_file_name)

   for stream in metadata.streams:
      if stream.isVideo():

         file_counter = stream.frames()
         video_framerate = file_counter/stream.durationSeconds()
         if not sound_of_silence:
            print("frame_count: {}".format(file_counter))
            print("framerate: {}".format(video_framerate))
         
         # create folder for frames
         if not os.path.exists(folder_to_put_frames):
            os.makedirs(folder_to_put_frames)
         
         ff = ffmpy.FFmpeg(
            inputs={video_file_name:None},
            outputs={"{}/out%4d.{}".format(folder_to_put_frames, extension):'-vf fps={}'.format(video_framerate)}
            )
         ff.run()
         break
   return video_framerate

def put_frames_together(video_file_name_we, folder, extension, video_framerate, audio_file_name):
   ff = ffmpy.FFmpeg(
   inputs={"{}/out%4d.{}".format(folder, extension):'-r {} -start_number 0001 -f image2'.format(video_framerate),
      audio_file_name:None},
   outputs={"{}_output.mp4".format(video_file_name_we):'-c:v libx264'}
   )
   # print ff.cmd
   ff.run()

def extract_audio(input_video_file_name, audio_name):
   ff = ffmpy.FFmpeg(
   inputs={video_file_name:None},
   outputs={audio_name:'-acodec copy'}#.format(audio_name)}
   )
   # print ff.cmd
   ff.run()

def get_first_frame(video_file_name, first_frame_name):
   ff = ffmpy.FFmpeg(
   inputs={video_file_name:None},
   outputs={first_frame_name:'-vf "select=gte(n\,1)" -vframes 1'}#.format(audio_name)}
   )
   # print ff.cmd
   ff.run()

   # ffmpeg -i test_video2.mp4 -vf "select=gte(n\,1)" -vframes 1 out_img.png


def create_jpeg_folder(input_folder, jpeg_folder):
   if not sound_of_silence:
      print ("creating jpeg")
   for file in os.listdir(input_folder):
      if file.endswith(".{}".format(extension)):
         file_name_we = os.path.splitext(file)[0]
         create_jpeg_from_png("{}/{}".format(input_folder, file), "{}/{}.jpeg".format(jpeg_folder, file_name_we))



def create_jpeg_from_png(input_file, output_file):
   from PIL import Image
   img = Image.open(input_file)
   img.save(output_file)

@timed_function
def workhorse(video_file_name):
   global sound_of_silence, debug, clean_up, use_jpeg
   #initiate names
   video_file_name_we = os.path.splitext(video_file_name)[0]
   temp_files_folder = "temp_{}".format(video_file_name_we)
   folder_with_input_frames = "{}/{}".format(temp_files_folder, "input_frames")
   folder_with_processed_frames = "{}/{}".format(temp_files_folder, "processed_frames")
   database_name = "{}/points.db".format(temp_files_folder)
   audio_file_name = "{}/{}_audio.aac".format(temp_files_folder, video_file_name_we)
   #older_with_grayscale = "{}/{}".format(temp_files_folder, "input_grayscale_frames")
   folder_with_jpeg = "{}/{}".format(temp_files_folder, "input_frames_jpeg")
   
   #create folders and database
   create_folder(temp_files_folder)
   create_folder(folder_with_input_frames)
   create_folder(folder_with_processed_frames)
   # create_folder(folder_with_grayscale)
   create_folder(folder_with_jpeg)
   database = create_database(database_name)
   
   #cut frames
   video_framerate = cut_video_on_frames(input_video_file_name=video_file_name, 
      folder_to_put_frames=folder_with_input_frames)

   extract_audio(video_file_name, audio_file_name)
   if use_jpeg:
      create_jpeg_folder(folder_with_input_frames, folder_with_jpeg)

   #analyze framase
   if use_jpeg:
      work_with_frames(folder_with_frames_name=folder_with_input_frames, 
         extension=extension, 
         api_servers_config_file=api_servers_config_file, 
         output_folder_name=folder_with_processed_frames,
         db_name=database,
         jpeg_folder=folder_with_jpeg)
   else:
      work_with_frames(folder_with_frames_name=folder_with_input_frames, 
         extension=extension, 
         api_servers_config_file=api_servers_config_file, 
         output_folder_name=folder_with_processed_frames,
         db_name=database)


   #assemble final video
   put_frames_together(video_file_name_we=video_file_name_we, 
      folder=folder_with_processed_frames, 
      extension=extension, 
      video_framerate=video_framerate,
      audio_file_name=audio_file_name)

   if clean_up:
      shutil.rmtree(temp_files_folder)

def looping_workhorse(video_file_queue):
   # while not self.stopped():
   while True: 
      mutex.acquire()
      if not video_file_queue.empty():
         video_file_name = video_file_queue.get()
         mutex.release()
         workhorse(video_file_name)
      else:
         mutex.release()
         time.sleep(0.5)



if __name__ == "__main__":
   # global sound_of_silence, debug, clean_up, use_jpeg

   if len(sys.argv) >1:
      if "-speak" in sys.argv:
         sound_of_silence = False
      if "-debug" in sys.argv:
         debug = True
      if "-clean" in sys.argv:
         clean_up = True
      if "-jpeg" in sys.argv:
         use_jpeg = True


   video_file_queue = Queue.Queue()
   

   # video_file_name = "HIMYMO.mp4"
   #video_file_name = "test_video2.mp4"

   # workhorse(video_file_name)

   s = socket.socket()
   PORT_NUMBER = 7331
   HOST_NAME = ""
   server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   server_socket.bind((HOST_NAME,PORT_NUMBER))
   server_socket.listen(5)


   looping_workhorse_thread = threading.Thread(target=looping_workhorse, args=( video_file_queue, ))
   # threads.append(t)
   looping_workhorse_thread.start()

   print("starting a server:")


   while True:
      client_socket, address = server_socket.accept()
      data = client_socket.recv(SIZE)
      print("reseeaved data: {}".format(data))
      request = json.loads(data)
      if request["request"] == "add_video_to_queue":
         print("adding_file to the queue: {}".format(request["video_file"]))
         video_file_name = request["video_file"]
         mutex.acquire()
         video_file_queue.put(video_file_name)
         mutex.release()

         get_first_frame(request["video_file"], "{}_first_frame.jpeg".format(os.path.splitext(video_file_name)[0]))

         respond_message = '{"respond": "gotcha"}'
         
   


      else :
         respond_message = '{"respond": "something went wrong, sorry..."}'
      client_socket.send(respond_message)












