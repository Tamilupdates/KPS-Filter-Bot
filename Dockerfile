FROM python:3.10.8

RUN apt install git -y

WORKDIR /app

COPY requirements.txt .
RUN pip3 install -U pip && pip3 install --ignore-installed --no-cache-dir -U -r requirements.txt

COPY . .

CMD ["bash", "start.sh"]