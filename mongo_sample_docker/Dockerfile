FROM ubuntu:20.04
RUN apt-get update
RUN apt-get install software-properties-common --yes
RUN apt-get install build-essential wget -y
RUN apt-get install python3-pip -y
RUN pip install --upgrade pip
RUN mkdir /qualdo
ADD ./sample_app.py /qualdo/sample_app.py
ADD ./requirements.txt /qualdo/requirements.txt
WORKDIR /qualdo
RUN pip install -r /qualdo/requirements.txt
ENV HOME /qualdo
EXPOSE 5000
CMD ["gunicorn", "-b", "0.0.0.0:5000", "sample_app:app", "--workers", "3", "--threads", "25", "--timeout", "660"]