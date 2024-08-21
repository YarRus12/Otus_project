FROM python:3.7

WORKDIR /app
COPY application/requirements.txt requirements.txt
RUN python -m pip install --upgrade pip && pip install -r requirements.txt
COPY application/ .

EXPOSE 8000

ENTRYPOINT ["gunicorn", "--bind", "0.0.0.0:8000", "wsgi:run"]