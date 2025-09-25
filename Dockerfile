FROM python:3.10.8-slim-buster

WORKDIR /usr/src/app
RUN chmod 777 /usr/src/app

RUN apt install git -y

COPY . .

RUN pip3 install -U pip && pip3 install --ignore-installed --no-cache-dir -U -r requirements.txt

CMD ["bash", "start.sh"]
