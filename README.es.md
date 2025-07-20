# TS-Tools replacement

Este repositorio contiene el código fuente de **TS-Tools replacement**, una biblioteca de Python diseñada para simplificar y optimizar la interacción con la **API de Tiendanube**. 
Su objetivo principal es facilitar la ejecución de requests (individuales o masivos), gestionando eficientemente los rate limits impuestos para cada tienda.

## Video Showcase (muy resumido)
https://drive.google.com/file/d/1TGpAv7oRqrUDhZMkNel1sRHxn-I1NuQ8/view?usp=sharing

## Instalación

Si bien el proyecto está en progreso y aún no está en completo, se encuentra en condiciones de que sus componentes sean utilizados.
Para poder instalarlo seguí los siguientes pasos:

1.  **Clona el repositorio:**
    ```bash
    git clone git@github.com:hugopessolano/TS-Tools-Replacement.git
    cd TS-Tools-replacement-repo
    ```

2.  **Crea y activa un entorno virtual:**
    Es una buena práctica usar entornos virtuales para aislar las dependencias del proyecto.

    *   En Linux/macOS:
        ```bash
        python3 -m venv venv
        source venv/bin/activate
        ```
    *   En Windows:
        ```bash
        python -m venv venv
        .\venv\Scripts\activate
        ```
    *(Verás `(venv)` al principio de la línea de comandos si la activación fue exitosa)*

3.  **Instala las dependencias:**
    El archivo `requirements.txt` contiene todas las bibliotecas necesarias.
    ```bash
    pip install -r requirements.txt
    ```

¡Listo! Ahora tienes el entorno configurado para empezar a usar o desarrollar la biblioteca.

## Documentación Técnica

La documentación técnica detallada del proyecto, incluyendo la descripción de cada módulo, las clases, funciones y los esquemas Pydantic utilizados, se encuentra generada en formato HTML.

GitHub Pages: https://hugopessolano.github.io/TS-Tools-Replacement/

Puedes acceder a ella abriendo el siguiente archivo en tu navegador web:

**docs/build/html/index.html**

Esta documentación es la referencia principal para entender la estructura interna y el uso de los diferentes componentes de la biblioteca.

![image](https://github.com/user-attachments/assets/76b303b8-b0df-47d4-8c45-c1eab45b3412)


## Acerca del Proyecto

**TS-Tools replacement** nace como una modernización y reemplazo de herramientas previas, enfocándose exclusivamente en la interacción con la API de Tiendanube. Está pensado principalmente para desarrolladores que necesiten crear scripts para extraer o manipular datos de Tiendanube de forma programática.

### Características Principales

*   **Interfaz Simplificada para API Tiendanube:** Abstrae complejidades para realizar llamadas a los endpoints de la API de forma más sencilla.
*   **Gestión Avanzada de Rate Limits:** Implementa una estrategia diseñada para maximizar el uso de la cuota de API, combinando un *burst* inicial con un ritmo sostenido, utilizando la librería `httpx` y sus semáforos. Detecta o permite configurar los límites específicos de cada tienda.
*   **Procesamiento de Datos con Pandas:** La herramienta hace uso de la librería de python `Pandas`, en conjunto con los tipos de datos diseñados especificamente para el almacenamiento de los requests y sus responses, para tener mayor eficiencia al momento de manipular grandes cantidades de datos. De momento no está siendo utilizado para persistir esos dataframes, pero deja espacio a la escalabilidad en ese aspecto. 
*   **Validación Robusta con Pydantic:** Utiliza extensivamente esquemas Pydantic para:
    *   Validar la configuración de la biblioteca.
    *   Definir y validar la estructura de los endpoints y las peticiones.
    *   Validar datos (potencialmente respuestas o entradas).
    *   Configurar los parámetros de rate limiting.
*   **Logging Persistente en Base de Datos:** Todas las operaciones de request, incluyendo parámetros, metadatos, éxito/fallo y los datos de respuesta, son registrados en una base de datos para auditoría, depuración y análisis posterior.
*   **Arquitectura Modular:** El código está organizado en módulos con responsabilidades claras (`request_manager`, `dataframe_manager`, `schemas`, `log_db`, etc.), facilitando su mantenimiento y extensión.

### Estado Actual

El proyecto se encuentra **en desarrollo activo**. La funcionalidad principal de la biblioteca (conexión, gestión de rate limits, logging) está implementada, pero aún falta desarrollar una capa superior de interacción (como una CLI o una interfaz gráfica) para el usuario final. Actualmente, está orientada a ser utilizada como un framework importado en scripts de Python. El manejo avanzado de errores y reintentos también está planificado para futuras versiones.

---

