FROM tiagopeixoto/graph-tool

RUN pacman -S --noconfirm python-pip
RUN pip install flask

COPY ./app /app
CMD ["python", "app/main.py"]