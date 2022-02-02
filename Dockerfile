FROM python:3.9-slim

WORKDIR /usr/src/app

RUN apt-get update && apt-get install curl --yes

RUN pip install --upgrade pip
RUN pip install --no-cache-dir poetry


RUN poetry config virtualenvs.create false
COPY poetry.lock pyproject.toml ./

RUN poetry install --no-dev

COPY . .

CMD ["uvicorn", "server:app" ]
