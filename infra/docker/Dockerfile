# Usa a imagem oficial
FROM apache/superset:latest

# Muda para root para ter permissão de instalar coisas
USER root

# 1. Instala o curl (caso não tenha) para baixar o instalador do pip
RUN apt-get update && apt-get install -y curl

# 2. Baixa o instalador do pip e instala DENTRO do ambiente virtual (.venv)
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    /app/.venv/bin/python get-pip.py

# 3. Usa o pip recém-instalado para colocar o driver do Druid
RUN /app/.venv/bin/pip install pydruid

# Volta para o usuário seguro do Superset
USER superset