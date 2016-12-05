from flask import Flask, render_template, request, url_for, Response, send_file
from do_the_Job import get_faces_points, draw_points_on_image
# Initialize the Flask application
import cv2, json, time
TEMP_FILE_NAME = "temp_file.jpg" 
global BUSY
BUSY = False
app = Flask(__name__)

# Define a route for the default URL, which loads the form
@app.route('/')
def form():
    return render_template('form_submit.html')

# Define a route for the action of the form, for example '/hello/'
# We are also defining which type of requests this route is 
# accepting: POST requests in this case
@app.route('/', methods=['POST'])
def accept_post_requests():
   global BUSY
   action = request.form['action']
    


   if action == "get_point_from_image_serial":
      BUSY = True
      file = request.files['img']
      file.save(TEMP_FILE_NAME)
      file.close()
   
      img_grey = cv2.imread(TEMP_FILE_NAME, cv2.IMREAD_GRAYSCALE);  

      try:
         faces_points = get_faces_points(img_grey) 
      except:
         print("something went wrong")
         faces_points = [[]]
      print "BULLET"
      time.sleep(2)

      dat = json.dumps(faces_points)
      resp = Response(response=dat,
         status=200, \
         mimetype="application/json")

      # show_img(img_grey)
      BUSY = False
      return(resp)


   if action == "draw_ponts_on_image":
      BUSY = True
      file = request.files['img']
      file.save(TEMP_FILE_NAME)
      file.close()

      points = json.loads(request.form['points'])
      # print "points here::" + points
      img = cv2.imread(TEMP_FILE_NAME)  
      # show_img(img)
      try:
         return_img = draw_points_on_image(img, points)
      except:
         print("something went wrong")
         return_img = img
      cv2.imwrite(TEMP_FILE_NAME, return_img);
      
      print "BULLET 2"


      BUSY = False
      return send_file(TEMP_FILE_NAME)

   if action == "check_if_ready_to_work":
      return "answer is {}".format(BUSY)

      # return(resp)
      # return flask.jsonify(**faces_points)
    
    # if 'img' not in request.files:
    #    print "lol no file"
    # else:
    #    print "got it !!!"
    

   return ('', 204)

def show_img(img):
      cv2.imshow('image',img)
      cv2.waitKey(0)
      # cv2.destroyAllWindows()


# Run the app :)
if __name__ == '__main__':
   app.run(
      debug=True, 
      threaded=True,
         host="0.0.0.0",
         port=int("1337")
   )
