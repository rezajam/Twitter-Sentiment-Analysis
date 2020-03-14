For Part A:
Twitter Client:
To exceute this program in Dockers
1) docker run -it -v ${PWD}:/app --name twitter -w /app python b
2) pip install -U git+https://github.com/tweepy/tweepy.git
3) python twitterA.py

App Client:
To execute this program in Dockers
1) docker run -it -v ${PWD}:/app --name server -p 5001:5001 python bash
2) pip install flask
3)python app.py

Spark Client:
To execute this program in Dockers
1)docker run -it -v ${PWD}:/app --link twitter:twitter --link server:server eecsyorku/eecs4415
2)spark-submit sparkA.py




For Part B:
Twitter Client:
To exceute this program in Dockers
1) docker run -it -v ${PWD}:/app --name twitter -w /app python bash
2) pip install -U git+https://github.com/tweepy/tweepy.git
3) python twitterB.py

App Client:
To execute this program in Dockers
1) docker run -it -v ${PWD}:/app --name server -p 5001:5001 python bash
2) pip install flask
3)python app.py

Spark Client:
To execute this program in Dockers
1)docker run -it -v ${PWD}:/app --link twitter:twitter --link server:server eecsyorku/eecs4415
2) pip install nltk
3)spark-submit sparkB.py
