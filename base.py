import json
import threading
import time
import redis
from redis_om import get_redis_connection, JsonModel, Field

# Configuración del cliente Redis
client = get_redis_connection(
    host='redis-11442.c336.samerica-east1-1.gce.redns.redis-cloud.com',
    port=11442,
    password='WHs8VBUND87TUF7UptNVK1tYxTsgmPPQ',
    decode_responses=True  # Para que las respuestas sean decodificadas como cadenas
)

# Verifica la conexión
try:
    client.ping()
    print("Conexión exitosa a Redis Cloud")
except redis.ConnectionError:
    print("Error al conectar con Redis Cloud")

# Definición del modelo Usuario
class User(JsonModel):
    username: str = Field(index=True)
    password: str  # Se recomienda almacenar contraseñas encriptadas
    first_name: str
    last_name: str
    email: str = Field(index=True)
    posts: str = Field(default="[]")  

    class Meta:
        database = client

# Modelo de Publicación
class Post(JsonModel):
    user_id: str = Field(index=True)  # ID del usuario que realizó la publicación
    content: str
    likes: int = 0  # Contador de likes

    class Meta:
        database = client

# Función para crear un nuevo usuario
def create_user(username: str, password: str, firstname: str, lastname: str, email: str) -> User:
    user = User(
        username=username,
        password=password,  
        first_name=firstname,
        last_name=lastname,
        email=email
    )
    user_id = f"user:{user.username}"  # Clave única para cada usuario
    client.hset(user_id, mapping=user.model_dump())  # Guardar el usuario como un hash en Redis

    return user

# Función para crear una nueva publicación
def create_post(user: User, content: str) -> Post:
    # Verificar que el objeto 'user' es una instancia de 'User'
    if not isinstance(user, User):
        raise ValueError("El parámetro 'user' debe ser un objeto de la clase User")

    post = Post(
        user_id="user:"+user.username,
        content=content,    
        likes=0
    )
    
    post_id = f"post:{post.pk}"  # Clave única para cada usuario
    client.hset(post_id, mapping=post.model_dump())


    user_key = f"user:{user.username}"  # Clave del usuario
    user_data = client.hgetall(user_key)  # Obtener los datos del usuario como un hash

    
    posts = json.loads(user_data['posts'])  # Deserializar el campo posts (JSON a lista)

    # Añadir el nuevo ID del post a la lista
    posts.append(post.pk)

    # Guardar la lista actualizada de posts en el hash del usuario
    client.hset(user_key, "posts", json.dumps(posts))  # Volver a serializar la lista como JSON
    
    return post

def obtener_usuarios():
    # Utiliza scan_iter para iterar sobre todas las claves que coincidan con el patrón
    for key in client.scan_iter(match="user:*"):
        print(key)  

def obtener_posts():
    # Utiliza scan_iter para iterar sobre todas las claves que coincidan con el patrón
    for key in client.scan_iter(match="post:*"):
        post_data = client.hgetall(key)
        
        print(key +"; "+post_data.get("user_id")) 

def sumar_like(post : Post):
    # Genera la clave del post
    post_key = f"post:{post.pk}"

    # Incrementar el número de likes en 1
    client.hincrby(post_key, "likes", 1)  # Esto incrementa el campo 'likes' en el hash del post

    # (Opcional) Obtener el número actual de likes después de incrementar
    current_likes = client.hget(post_key, "likes")
    print(f"El post {post_key} ahora tiene {current_likes} likes.")    





#test likes hardcodeado
POST_ID = "post:post1"
TOTAL_LIKES = 10000  # Número total de likes a simular
THREADS = 11       # Número de threads simulando usuarios concurrentes
def test():

# Inicializa el contador de likes en la publicación
    client.hset(POST_ID, "likes", 0)

    # Crea los threads
    start_time = time.time()
    threads = []
    for _ in range(THREADS):
        thread = threading.Thread(target=increment_likes)
        threads.append(thread)
        thread.start()

    # Espera a que todos los threads finalicen
    for thread in threads:
        thread.join()

    end_time = time.time()

    # Imprime el resultado final
    likes = client.hget(POST_ID, "likes")
    print(f"Total likes: {int(likes)}")
    print(f"Tiempo total de prueba: {end_time - start_time} segundos")

# Función para incrementar los likes
def increment_likes():
    for _ in range(TOTAL_LIKES // THREADS):
        client.hincrby(POST_ID, "likes", 1)

test()