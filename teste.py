import requests

# URL del servidor (cambia por tu IP)
URL = "http://raspberrypi-1:5000/process"

# Ruta de la imagen
IMAGE_PATH = "teste.jpg"  # cambia esto

def send_image():
    with open(IMAGE_PATH, "rb") as f:
        files = {
            "image": ("image.jpg", f, "image/jpeg")
        }

        data = {
            "force_detect": "1"  # opcional
        }

        response = requests.post(URL, files=files, data=data)

        print("Status code:", response.status_code)

        try:
            print("Respuesta JSON:")
            print(response.json())
        except:
            print("Respuesta RAW:")
            print(response.text)


if __name__ == "__main__":
    send_image()