import sys, sqlite3, json, atexit
from loguru import logger
from logger_settings import STDOUT_LOGGER_SETTINGS, DB_STDOUT_LOGGER_SETTINGS, DB_LOGGER_SETTINGS, DB_ONLY_LOGGER_SETTINGS
DB_FILE = "execution_logs.db"

class SQLiteSink:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self._connect()
        self._setup_table()
        atexit.register(self.close)

    def _connect(self):
        """Establece la conexión con la base de datos."""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.cursor = self.conn.cursor()
            #print(f"Conectado a la base de datos SQLite: {self.db_path}")
        except sqlite3.Error as e:
            print(f"Error al conectar con SQLite DB '{self.db_path}': {e}", file=sys.stderr)
            self.conn = None
            self.cursor = None

    def _setup_table(self):
        """Crea la tabla de logs si no existe."""
        if not self.cursor: return
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_id TEXT,
                    timestamp TEXT NOT NULL,
                    level_name TEXT NOT NULL,
                    level_no INTEGER NOT NULL,
                    message TEXT,
                    module TEXT,
                    funcName TEXT,
                    line INTEGER,
                    extra TEXT
                )
            """)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error al crear/verificar la tabla 'logs': {e}", file=sys.stderr)

    def write(self, message):
        if not self.conn or not self.cursor:
            print("Saltando log a SQLite: No hay conexión a la base de datos.", file=sys.stderr)
            return
        try:
            # Parsea el registro JSON enviado por Loguru
            record = json.loads(message)['record']

            log_entry = {
                'operation_id': record.get('extra', {}).get('operation_id', None),
                'timestamp': record['time']['repr'],
                'level_name': record['level']['name'],
                'level_no': record['level']['no'],
                'message': record['message'],
                'module': record['module'],
                'funcName': record['function'],
                'line': record['line'],
                'extra': json.dumps(record.get('extra', {}))
            }
            self.cursor.execute("""
                INSERT INTO logs (operation_id, timestamp, level_name, level_no, message, module, funcName, line, extra)
                VALUES (:operation_id, :timestamp, :level_name, :level_no, :message, :module, :funcName, :line, :extra)
            """, log_entry)
            self.conn.commit()

        except json.JSONDecodeError as e:
            print(f"Error al decodificar el mensaje JSON del log: {e}", file=sys.stderr)
            print(f"Data recibida: {message}", file=sys.stderr)
        except sqlite3.Error as e:
            print(f"Error al escribir log en SQLite DB: {e}", file=sys.stderr)
            # try: self.conn.rollback() except: pass
        except Exception as e:
            print(f"Error inesperado en SQLiteSink: {e}", file=sys.stderr)

    def close(self):
        """Cierra la conexión a la base de datos."""
        if self.conn:
            try:
                self.conn.commit()
                self.conn.close()
                print(f"Conexión SQLite '{self.db_path}' cerrada.")
                self.conn = None
                self.cursor = None
            except sqlite3.Error as e:
                print(f"Error al cerrar la conexión SQLite: {e}", file=sys.stderr)

    def __call__(self, message):
        """Hace que la instancia de la clase sea callable para logger.add."""
        self.write(message)

logger.remove()
def is_stdout_only(record):
    """Filtro para logs que SÓLO van a stdout."""
    return record["extra"].get("logger_id") == "stdout_only"

def is_db_and_stdout(record):
    """Filtro para logs que van a stdout Y a la BD."""
    return record["extra"].get("logger_id") == "db_and_stdout"

def is_db(record):
    """Filtro para logs que van a stdout Y a la BD."""
    return record["extra"].get("logger_id") == "db_only"

logger.add(
    sys.stdout,
    level=STDOUT_LOGGER_SETTINGS.level,
    format=STDOUT_LOGGER_SETTINGS.format,
    filter=is_stdout_only,
    colorize=STDOUT_LOGGER_SETTINGS.colorize
)
logger.add(
    sys.stdout,
    level=DB_STDOUT_LOGGER_SETTINGS.level,
    format=DB_STDOUT_LOGGER_SETTINGS.format,
    filter=is_db_and_stdout,
    colorize=DB_STDOUT_LOGGER_SETTINGS.colorize
)

sqlite_sink = SQLiteSink(DB_FILE)
logger.add(
    sqlite_sink,
    level=DB_LOGGER_SETTINGS.level,
    filter=is_db_and_stdout,
    serialize=DB_LOGGER_SETTINGS.serialize,
    catch=DB_LOGGER_SETTINGS.catch
)
sqlite_sink2 = SQLiteSink(DB_FILE)
logger.add(
    sqlite_sink2,
    level=DB_ONLY_LOGGER_SETTINGS.level,
    filter=is_db,
    serialize=DB_ONLY_LOGGER_SETTINGS.serialize,
    catch=DB_ONLY_LOGGER_SETTINGS.catch
)
#logger.add(sys.stdout,  colorize=True, format="<green>{time:YYYY/MM/DD - HH:mm:ss}</green> <level>{level: <8}</level> <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

stdout_logger = logger.bind(logger_id="stdout_only")
db_logger = logger.bind(logger_id="db_and_stdout")
db_only_logger = logger.bind(logger_id="db_only")


# --- Ejemplo de Uso ---
if __name__ == '__main__':
    print("--- Iniciando prueba de loggers ---")

    stdout_logger.info("Este mensaje va SOLO a la consola (stdout_logger).")
    stdout_logger.debug("Este mensaje DEBUG también va SOLO a la consola.")

    db_logger.info("Este mensaje va a la consola Y a la base de datos (db_logger).")
    db_logger.warning("Esta advertencia también va a ambos sitios.")
    db_logger.debug("Este mensaje DEBUG de db_logger NO aparecerá en la BD (nivel INFO), pero SÍ en consola (porque el sink de stdout para db_logger lo permite).")
    
    stdout_logger.bind(user="Hugo").info("Acción específica de Hugo (solo consola).")
    db_logger.bind(request_id="xyz789").error("Error en request (consola y BD).")
