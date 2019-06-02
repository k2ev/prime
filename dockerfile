FROM ubuntu:18.04
RUN apt-get update &&  apt-get install -y \
    python3-pip \
    python3.7 \
    vim \
&& rm -rf /var/lib/apt/lists/*

COPY ./prime/ /prime 
WORKDIR /prime
RUN pip3 install -r requirements.txt

CMD ["/usr/bin/python3", "run.py"]
