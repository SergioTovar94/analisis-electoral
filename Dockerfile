FROM python:3.11

WORKDIR /app

# Copia requirements.txt e instala las librerías
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Monta la carpeta de tu proyecto
COPY . /app

# Mantener el contenedor en modo interactivo
CMD ["tail", "-f", "/dev/null"]
