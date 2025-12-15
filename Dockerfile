FROM python:3.12.11

WORKDIR /workspace/app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-m", "src.main"]