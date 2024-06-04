FROM python:3.10.12-alpine3.18

WORKDIR /usr/app

ADD bubble.py .

RUN pip install proxy-protocol crc32c

CMD [ "python", "/usr/app/bubble.py", "/usr/app/config.ini" ]
