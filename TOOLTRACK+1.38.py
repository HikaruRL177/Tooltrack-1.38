#!/usr/bin/env python
# -*- coding: utf-8-sig -*-
#& C:/Users/gdlgusre/Documents/Producto/Arista/Apps/TOOL/Tool_env/Scripts/Activate.ps1
#cd C:\Users\gdlgusre\Documents\Producto\Arista\Apps\Tool\tooltrack_plus
#python manage.py runserver
#http://127.0.0.1:8000/api/modules_config/?email=gustavo.reyna@flex.com
#python c:/Users/gdlgusre/Documents/Producto/Arista/Apps/TOOL/Tool1.0.py
#pyinstaller --onefile --windowed --distpath="\\gdlnt104\LABELCONFIG\LABELS\B18\TOOL\ToolTrack+" --icon="\\gdlnt104\LABELCONFIG\LABELS\B18\TOOL\ToolTrack+\Recursos\LOGOS\ToolTrack-_logo2.ico" TOOLTRACK+1.38.py

import json
import sys
import os
import webbrowser
import csv
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from PyQt5 import QtWidgets, QtGui, QtCore
from PyQt5.QtCore import QThread, pyqtSignal, QDate # QUrl and QDesktopServices removed
import re
import time
try:
    from filelock import FileLock, Timeout as FileLockTimeout # Importar Timeout como FileLockTimeout
except ImportError:
    print("ADVERTENCIA: La librería 'filelock' no está instalada. El bloqueo de archivos estará deshabilitado.")
    print("Instálala con: pip install filelock")
    # Crear clases dummy para evitar errores si no está instalado
    class FileLock:
        def __init__(self, path, timeout=1): pass
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): pass
        def acquire(self, timeout=None): return self # Simular adquisición
        def release(self): pass # Simular liberación
    class FileLockTimeout(Exception): # Crear excepción dummy
        pass


import shutil # Para copiar iconos si es necesario
import traceback # Para imprimir errores detallados

# --- Windows API Imports ---
# Make sure to install pywin32: pip install pywin32
try:
    import win32api
    import win32con
except ImportError:
    print("Error: La librería 'pywin32' no está instalada.")
    print("Por favor, instálala ejecutando: pip install pywin32")
    # Optionally, display a message box and exit if running in GUI mode later
    # QtWidgets.QMessageBox.critical(None, "Error Crítico", "La librería 'pywin32' es necesaria y no está instalada.\nInstálala con: pip install pywin32")
    sys.exit(1) # Exit if pywin32 is crucial and not found

# --- Rutas de archivos ---
BASE_NETWORK_PATH = r"\\gdlnt104\ScanDirs\B18\ToolTrack+\Recursos"
DB_NETWORK_PATH = os.path.join(BASE_NETWORK_PATH, "DB")
BASE_LOCAL_PATH = r"C:\TOOLTRACK+\Recursos" # Nueva ruta base local
DB_LOCAL_PATH = os.path.join(BASE_LOCAL_PATH, "DB") # Nueva ruta DB local

DB_PATH = os.path.join(DB_NETWORK_PATH, "MANTENIMIENTO_HERRAMENTALES_TOOLTRACK+.csv")
USERS_DB_PATH = os.path.join(DB_NETWORK_PATH, "USERS_TOOLTRACK+.csv")
HISTORY_PATH = os.path.join(DB_NETWORK_PATH, "HISTORY_TOOLTRACK+.csv")
EXPIRATION_PATH = os.path.join(DB_NETWORK_PATH, "SKID_TOOLTRACK+.csv")
INVENTORY_DB_PATH = EXPIRATION_PATH
FU_DB_PATH = os.path.join(DB_NETWORK_PATH, "PARAMETERS.csv")
FORECAST_BD_PATH = os.path.join(DB_NETWORK_PATH, "FORECAST.csv")
CHECKLIST_PATH = os.path.join(BASE_NETWORK_PATH, "checklists.json") # Asumiendo que está en Recursos directamente
CATALOGO_PATH = os.path.join(DB_NETWORK_PATH, "CATALOGO_TOOLTRACK+.xlsx")
IMAGENES_CAT_PATH = os.path.join(DB_NETWORK_PATH, "Imagenes_Catalogo") # Ruta de red para imágenes
DASHBOARD_PATH = os.path.join(DB_NETWORK_PATH, "EXPIRATION_TOOLTRACK+.csv")
CONSUMABLE_INVENTORY_PATH = os.path.join(DB_NETWORK_PATH, "CONSUMABLE_INVENTORY_TOOLTRACK+.csv")
STOCK_LOG_PATH = os.path.join(DB_NETWORK_PATH, "STOCK_LOG_TOOLTRACK+.csv")
# --- NUEVA RUTA LOCAL PARA IMÁGENES ---
LOCAL_IMAGENES_CAT_PATH = os.path.join(DB_LOCAL_PATH, "Imagenes_Catalogo")

# --- Iconos y Logos (se pueden mantener locales o sincronizar también si es necesario) ---
LOCAL_ICON_PATH = os.path.join(BASE_LOCAL_PATH, "Iconos_Botones")
LOCAL_LOGO_PATH = os.path.join(BASE_LOCAL_PATH, "LOGOS")
# Asegurarse de que las carpetas locales existan (se hará también en la sincronización)
os.makedirs(DB_LOCAL_PATH, exist_ok=True)
os.makedirs(LOCAL_IMAGENES_CAT_PATH, exist_ok=True)
os.makedirs(LOCAL_ICON_PATH, exist_ok=True)
os.makedirs(LOCAL_LOGO_PATH, exist_ok=True)
# -----------------------------------------

# --- Constantes ---
EXPIRATION_ALERT_DAYS = 30 # Días antes de la expiración para considerar "próximo a vencer"

# --- Caché para datos de usuario ---
USER_DATA_CACHE = None
MODULE_WIDGET_CACHE = {} # Caché para instancias de widgets (Lazy Loading)


###############################################################################
# Clase Session (sin cambios)
###############################################################################
class Session:
    user_alias = None
    user_data = None
    allowed_modules = None # Inicializar
    initial_widget_instance = None # Para pre-instancia
    initial_widget_index = -1      # Para pre-instancia

###############################################################################
# Función para cargar información de usuario (sin cambios respecto a la versión anterior)
###############################################################################
def load_user_data_by_email(email):
    global USER_DATA_CACHE
    if USER_DATA_CACHE is None:
        print("Cargando caché de datos de usuario...")
        USER_DATA_CACHE = {}
        if not os.path.exists(USERS_DB_PATH):
            print(f"Error: El archivo {USERS_DB_PATH} no existe.")
            # Podrías lanzar una excepción aquí si el archivo es crítico
            return None
        try:
            # Usar pandas para lectura eficiente, especialmente si el archivo es grande
            # keep_default_na=False evita que strings vacíos se lean como NaN
            df_users = pd.read_csv(USERS_DB_PATH, encoding="utf-8-sig", keep_default_na=False, dtype=str) # Leer todo como string inicialmente
            # Normalizar nombres de columnas a mayúsculas para consistencia
            df_users.columns = [col.upper() for col in df_users.columns]

            if "CORREO" not in df_users.columns:
                print(f"Error: La columna 'CORREO' no se encontró en {USERS_DB_PATH}")
                return None # O manejar el error como prefieras

            # Convertir a diccionario indexado por correo (normalizado a minúsculas)
            for _, row in df_users.iterrows():
                # Acceder a la columna CORREO y asegurar que sea string antes de procesar
                user_email_val = row.get("CORREO", "")
                if pd.isna(user_email_val): # Chequeo explícito por si acaso
                    user_email_val = ""

                user_email = str(user_email_val).strip().lower()
                if user_email:
                    # Convertir la fila (Series de Pandas) a un diccionario estándar de Python
                    USER_DATA_CACHE[user_email] = row.to_dict()

            print(f"Caché de usuarios cargada con {len(USER_DATA_CACHE)} entradas.")
        except pd.errors.EmptyDataError:
            print(f"Advertencia: El archivo {USERS_DB_PATH} está vacío.")
            USER_DATA_CACHE = {} # Asegurar que el caché esté vacío
        except Exception as e:
            print(f"Error crítico al cargar o procesar {USERS_DB_PATH}: {e}")
            USER_DATA_CACHE = {} # Resetear caché en caso de error
            # Podrías lanzar una excepción o mostrar un mensaje al usuario
            return None

    # Buscar en el caché (O(1) en promedio)
    normalized_email = email.strip().lower()
    print(f"Buscando email normalizado '{normalized_email}' en caché.")
    user_info = USER_DATA_CACHE.get(normalized_email)
    if user_info:
        print("Usuario encontrado en caché.")
    else:
        print("Usuario NO encontrado en caché.")
    return user_info


###############################################################################
# NUEVA Función para sincronizar imágenes
###############################################################################
def synchronize_images(source_dir, dest_dir, progress_callback=None):
    """
    Sincroniza archivos de imagen desde source_dir (red) a dest_dir (local).
    Copia archivos si no existen localmente o si la versión de la red es más nueva.
    Args:
        source_dir (str): Directorio de origen (red).
        dest_dir (str): Directorio de destino (local).
        progress_callback (callable, optional): Función para reportar progreso (0-100).
    Returns:
        bool: True si la sincronización fue exitosa (o no necesaria), False si hubo errores.
    """
    print(f"Iniciando sincronización de imágenes de '{source_dir}' a '{dest_dir}'")
    if not os.path.isdir(source_dir):
        print(f"Error: El directorio de origen '{source_dir}' no existe o no es accesible.")
        return False

    try:
        # Crear directorio de destino si no existe
        os.makedirs(dest_dir, exist_ok=True)
        print(f"Directorio local '{dest_dir}' asegurado.")
    except OSError as e:
        print(f"Error crítico: No se pudo crear el directorio local '{dest_dir}': {e}")
        return False

    try:
        source_files = [f for f in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir, f))]
        total_files = len(source_files)
        print(f"Se encontraron {total_files} archivos en el origen.")
        copied_count = 0
        skipped_count = 0
        error_count = 0

        for i, filename in enumerate(source_files):
            source_path = os.path.join(source_dir, filename)
            dest_path = os.path.join(dest_dir, filename)
            should_copy = False

            try:
                if not os.path.exists(dest_path):
                    should_copy = True
                    # print(f"Archivo '{filename}' no existe localmente.")
                else:
                    source_mtime = os.path.getmtime(source_path)
                    dest_mtime = os.path.getmtime(dest_path)
                    if source_mtime > dest_mtime:
                        should_copy = True
                        # print(f"Archivo '{filename}' es más nuevo en la red.")
                    # else:
                        # print(f"Archivo '{filename}' local está actualizado.")

                if should_copy:
                    print(f"Copiando '{filename}' a local...")
                    shutil.copy2(source_path, dest_path) # copy2 preserva metadatos como mtime
                    copied_count += 1
                else:
                    skipped_count += 1

            except Exception as e_file:
                print(f"Error al procesar/copiar el archivo '{filename}': {e_file}")
                error_count += 1

            # Reportar progreso
            if progress_callback and total_files > 0:
                progress = int(((i + 1) / total_files) * 100)
                progress_callback(progress) # Llama a la función de callback con el progreso

        print(f"Sincronización completada. Copiados: {copied_count}, Omitidos: {skipped_count}, Errores: {error_count}")
        return error_count == 0 # Exitoso si no hubo errores

    except Exception as e:
        print(f"Error durante la sincronización de imágenes: {e}")
        traceback.print_exc() # Imprime el traceback completo para depuración
        return False


###############################################################################
# Worker para obtener UPN de Windows (sin cambios)
###############################################################################
class WindowsLoginWorker(QtCore.QThread):
    """
    Worker que obtiene el User Principal Name (UPN) del usuario logueado
    en Windows y lo valida contra el archivo CSV.
    """
    loginResult = pyqtSignal(dict)      # Emite {'email': upn} en éxito
    errorOccurred = pyqtSignal(str)     # Emite mensaje de error
    progressChanged = pyqtSignal(int)   # Emite progreso (0-100)

    def __init__(self, parent=None):
        super().__init__(parent)

    def run(self):
        self.progressChanged.emit(10) # Iniciando
        print("Intentando obtener UPN del usuario de Windows...")
        try:
            # --- Obtener UPN usando win32api ---
            # El formato 8 corresponde a NameUserPrincipal
            upn = win32api.GetUserNameEx(8) # O usar win32con.NameUserPrincipal si se importa
            print(f"UPN obtenido de Windows: {upn}")
            self.progressChanged.emit(50) # UPN obtenido

            if not upn or '@' not in upn:
                raise ValueError("No se pudo obtener un UPN válido (formato email) de Windows.")

            # Consideramos el UPN como el 'email' para la validación
            self.loginResult.emit({"email": upn})
            # El progreso al 100% se emitirá después de la sincronización en on_login_result

        except Exception as e:
            error_msg = f"Error al obtener usuario de Windows: {e}"
            print(error_msg)
            # Intentar obtener nombre de usuario simple como fallback? Podría ser útil para logs.
            try:
                username = win32api.GetUserName()
                error_msg += f"\n(Nombre de usuario simple: {username})"
            except Exception:
                pass # Ignorar si GetUserName también falla
            self.errorOccurred.emit(error_msg)
            self.progressChanged.emit(0) # Resetear progreso en error


###############################################################################
# Clase UserLoginDialog (MODIFICADA para sincronización y barra unificada)
###############################################################################
class UserLoginDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowFlags(QtCore.Qt.FramelessWindowHint | QtCore.Qt.WindowStaysOnTopHint)
        self.setWindowTitle("Verificación de Usuario")
        # self.resize(350, 300) # El tamaño se ajustará con setMinimumSize
        self.setModal(True)
        self.setAttribute(QtCore.Qt.WA_TranslucentBackground)
        self._setup_ui()
        self._drag_pos = None
        self.login_worker = None
        self.user_alias = None

        # --- Variables para medir tiempo ---
        self.time_start_login = 0
        self.time_upn_received = 0
        self.time_csv_loaded = 0
        self.time_sync_start = 0
        self.time_sync_end = 0
        self.time_modules_processed = 0
        # ---------------------------------

    def _setup_ui(self):
        """Configura la interfaz gráfica del diálogo de login."""
        main_layout = QtWidgets.QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        self.frame = QtWidgets.QFrame()
        self.frame.setObjectName("loginFrame")
        self.frame.setStyleSheet("""
            #loginFrame {
                background-color: rgba(255, 255, 255, 230); /* Fondo semi-transparente */
                border-radius: 15px; /* Bordes redondeados */
            }
        """)
        frame_layout = QtWidgets.QVBoxLayout(self.frame)
        frame_layout.setContentsMargins(20, 20, 20, 20) # Márgenes internos
        frame_layout.setSpacing(15) # Espacio entre widgets

        # --- Imagen de usuario ---
        self.user_image = QtWidgets.QLabel()
        self.user_image.setAlignment(QtCore.Qt.AlignCenter)
        user_icon_path = os.path.join(LOCAL_LOGO_PATH, "user_icon.png")

        # Intentar copiar el icono si no existe localmente (simple fallback)
        # Esto debería hacerse idealmente en la instalación o al inicio global de la app
        network_user_icon = r"\\gdlnt104\ScanDirs\B18\ToolTrack+\Recursos\LOGOS\user_icon.png"
        if not os.path.exists(user_icon_path) and os.path.exists(network_user_icon):
            try:
                os.makedirs(os.path.dirname(user_icon_path), exist_ok=True)
                shutil.copy2(network_user_icon, user_icon_path)
                print(f"Icono de usuario copiado a '{user_icon_path}'")
            except Exception as e:
                print(f"No se pudo copiar el icono de usuario: {e}")

        # Cargar el icono
        if os.path.exists(user_icon_path):
             pixmap = QtGui.QPixmap(user_icon_path)
             if not pixmap.isNull():
                 self.user_image.setPixmap(pixmap.scaled(80, 80,
                                                         QtCore.Qt.KeepAspectRatio,
                                                         QtCore.Qt.SmoothTransformation))
             else:
                 self.user_image.setText("?") # Placeholder si la imagen es inválida
        else:
             self.user_image.setText("?") # Placeholder si el archivo no existe

        frame_layout.addWidget(self.user_image)

        # --- Botón para iniciar verificación ---
        self.login_button = QtWidgets.QPushButton("Verificar Usuario")
        self.login_button.setObjectName("loginButton")
        self.login_button.setFixedHeight(40)
        self.login_button.setFixedWidth(200)
        self.login_button.setStyleSheet("""
            QPushButton#loginButton {
                background-color: #893c91; /* Color principal */
                color: white;
                border: none;
                border-radius: 20px; /* Botón redondeado */
                font-size: 16px;
                font-weight: bold;
            }
            QPushButton#loginButton:hover {
                background-color: #6d3075; /* Color ligeramente más oscuro al pasar el mouse */
            }
            QPushButton#loginButton:pressed {
                background-color: #53245a; /* Color al presionar */
            }
            QPushButton#loginButton:disabled {
                background-color: #cccccc; /* Color cuando está deshabilitado */
                color: #666666;
            }
        """)
        self.login_button.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor)) # Cursor de mano
        self.login_button.clicked.connect(self.start_login)
        frame_layout.addWidget(self.login_button, alignment=QtCore.Qt.AlignCenter)

        # --- Barra de progreso UNIFICADA ---
        self.progress_bar_total = QtWidgets.QProgressBar()
        self.progress_bar_total.setRange(0, 100) # Rango total de 0 a 100
        self.progress_bar_total.setValue(0)
        self.progress_bar_total.setFixedHeight(18) # Un poco más alta
        self.progress_bar_total.setTextVisible(False) # Ocultar el texto de porcentaje
        progress_style_total = """
            QProgressBar {
                border: 1px solid #b0b0b0; /* Borde gris */
                border-radius: 7px;      /* Más redondeado */
                text-align: center;
                background-color: #f0f0f0; /* Fondo gris muy claro */
                height: 16px; /* Altura interna */
            }
            QProgressBar::chunk {
                background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #ab47bc, stop:1 #8e24aa); /* Gradiente morado */
                /* background-color: #893c91; */ /* Color sólido inicial */
                border-radius: 6px; /* Bordes redondeados para el chunk */
                margin: 1px; /* Pequeño margen interno */
            }
        """
        self.progress_bar_total.setStyleSheet(progress_style_total)
        frame_layout.addWidget(self.progress_bar_total) # Añadir la barra única
        # --- Fin Barra de progreso UNIFICADA ---

        # --- Label para mensajes ---
        self.message_label = QtWidgets.QLabel("TOOLTRACK+")
        self.message_label.setAlignment(QtCore.Qt.AlignCenter)
        self.message_label.setWordWrap(True) # Para textos largos
        self.message_label.setStyleSheet("font-size: 14pt; color: #333;") # Tamaño y color
        self.message_label.setMinimumHeight(40) # Altura mínima para evitar saltos
        frame_layout.addWidget(self.message_label)

        main_layout.addWidget(self.frame)
        # Ajustar tamaño mínimo del diálogo basado en el contenido
        self.frame.setMinimumSize(350, 300) # Ajustar según sea necesario
        self.adjustSize() # Ajustar el tamaño del diálogo al contenido inicial


    def update_message(self, text):
        """Actualiza el texto del label de mensajes."""
        # No cambia el tamaño de fuente, usa wordWrap
        self.message_label.setText(text)


    # --- Eventos de ventana para mover sin marco ---
    def mousePressEvent(self, event):
        """Captura la posición inicial al hacer clic para arrastrar."""
        if event.button() == QtCore.Qt.LeftButton:
            # Guardar la diferencia entre la posición global del clic y la esquina superior izquierda de la ventana
            self._drag_pos = event.globalPos() - self.frameGeometry().topLeft()
            event.accept()

    def mouseMoveEvent(self, event):
        """Mueve la ventana si se está arrastrando con el botón izquierdo presionado."""
        # Verificar si se está presionando el botón izquierdo y si hay una posición de arrastre guardada
        if event.buttons() == QtCore.Qt.LeftButton and self._drag_pos:
            # Mover la ventana a la nueva posición global menos el offset guardado
            self.move(event.globalPos() - self._drag_pos)
            event.accept()
    # -----------------------------------------


    def start_login(self):
        """Inicia el proceso de verificación de usuario."""
        self.time_start_login = time.time()
        self.login_button.setText("Verificando...")
        self.login_button.setEnabled(False)
        self.progress_bar_total.setValue(0) # Resetear barra única
        self.update_message("Obteniendo usuario local...")

        # Crear e iniciar el worker para obtener el UPN de Windows
        self.login_worker = WindowsLoginWorker()
        self.login_worker.loginResult.connect(self.on_login_result)
        self.login_worker.errorOccurred.connect(self.on_login_error)
        # Conectar la señal de progreso del worker a la función que actualiza la primera mitad de la barra
        self.login_worker.progressChanged.connect(self.update_auth_progress)
        self.login_worker.start()


    def update_auth_progress(self, value):
        """Actualiza la primera mitad (0-50) de la barra de progreso total."""
        # Mapea el valor (0-100) de la autenticación/carga a la primera mitad (0-50) del total
        total_progress_value = int(value * 0.5) # 50% del peso total para esta fase
        self.progress_bar_total.setValue(total_progress_value)

        # Opcional: Cambiar el color del chunk si se desea indicar la fase
        # style = self.progress_bar_total.styleSheet()
        # new_style = style.replace("background-color: #5a913c;", "background-color: #893c91;") # Asegura color morado
        # self.progress_bar_total.setStyleSheet(new_style)

        # Actualizar mensajes según el progreso de la verificación local/carga CSV
        if value <= 10:
            self.update_message("Iniciando verificación...")
        elif value <= 50:
            self.update_message("Obteniendo usuario de Windows...")
        elif value < 70: # Después de obtener UPN, antes de cargar CSV
            self.update_message("Usuario obtenido, verificando autorización...")
        elif value < 90: # Durante la carga de CSV/permisos
             self.update_message("Cargando perfil y permisos...")
        # El resto de mensajes se manejan en on_login_result y update_sync_progress


    def update_sync_progress(self, value):
        """Actualiza la segunda mitad (50-100) de la barra de progreso total."""
        # Mapea el valor (0-100) de la sincronización a la segunda mitad (50-100) del total
        # Empieza en 50 (fin estimado de la autenticación/carga) y añade el progreso de sync (0-50)
        total_progress_value = 50 + int(value * 0.5) # 50% del peso total para esta fase
        # Asegurar que no exceda 100
        total_progress_value = min(total_progress_value, 100)
        self.progress_bar_total.setValue(total_progress_value)

        # Opcional: Cambiar color del chunk a verde durante la sincronización
        # style = self.progress_bar_total.styleSheet()
        # # Usar un gradiente verde
        # new_style = style.replace("qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #ab47bc, stop:1 #8e24aa)",
        #                           "qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #66bb6a, stop:1 #43a047)")
        # self.progress_bar_total.setStyleSheet(new_style)

        # Actualizar mensaje
        if value < 100:
             self.update_message(f"Sincronizando imágenes ({value}%)...")
        else:
             self.update_message("Sincronización completada.")
             # Opcional: Restaurar color original al finalizar
             # style = self.progress_bar_total.styleSheet()
             # new_style = style.replace("qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #66bb6a, stop:1 #43a047)",
             #                           "qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #ab47bc, stop:1 #8e24aa)")
             # self.progress_bar_total.setStyleSheet(new_style)


    def on_login_result(self, result):
        """Maneja el resultado exitoso del worker de login."""
        self.time_upn_received = time.time()
        upn_as_email = result.get("email")
        print(f"UPN/Email verificado localmente: {upn_as_email}")
        # El progreso de obtención de UPN ya se actualizó a ~50% (del 50% total)

        try:
            # --- Carga de datos del usuario (con caché) ---
            self.update_message("Cargando datos de usuario...") # Mensaje
            start_load_user = time.time()
            # Cargar datos del usuario usando la función externa
            Session.user_data = load_user_data_by_email(upn_as_email)
            end_load_user = time.time()
            self.time_csv_loaded = time.time()
            print(f"Tiempo load_user_data_by_email: {end_load_user - start_load_user:.4f} segundos")
            # Actualizar progreso auth (ej. al 75% de su parte -> ~37.5% total)
            self.update_auth_progress(75) # Esto pondrá la barra total en ~37

            # Verificar si se encontraron datos del usuario
            if Session.user_data:
                # Obtener alias, con fallback al nombre de usuario del email
                self.user_alias = Session.user_data.get("ALIAS", upn_as_email.split('@')[0])
                Session.user_alias = self.user_alias # Guardar en la sesión global
                print("Alias asignado:", self.user_alias)
                # Actualizar progreso auth (ej. al 90% de su parte -> 45% total)
                self.update_auth_progress(90) # Esto pondrá la barra total en 45

                # --- <<< INICIO: Sincronización de Imágenes >>> ---
                self.time_sync_start = time.time()
                self.update_message("Iniciando sincronización de imágenes...")
                # La barra de sync empieza implícitamente en 50% del total
                # Llamar a la función externa de sincronización, pasando el callback de progreso
                sync_successful = synchronize_images(
                    IMAGENES_CAT_PATH,      # Origen (red)
                    LOCAL_IMAGENES_CAT_PATH,# Destino (local)
                    self.update_sync_progress # Callback para actualizar la barra (50-100)
                )
                # Asegurarse que la barra llegue al 100% al final de sync,
                # independientemente de si fue exitosa o no.
                self.progress_bar_total.setValue(100)

                self.time_sync_end = time.time()
                print(f"Tiempo de sincronización de imágenes: {self.time_sync_end - self.time_sync_start:.4f} segundos")
                # Mostrar advertencia si hubo errores, pero no detener el proceso
                if not sync_successful:
                    print("Advertencia: Ocurrieron errores durante la sincronización de imágenes.")
                    # Usar un QMessageBox no modal o un mensaje menos intrusivo si se prefiere
                    QtWidgets.QMessageBox.warning(self, "Advertencia de Sincronización",
                                                  "No se pudieron sincronizar todas las imágenes del catálogo.\n"
                                                  "Algunas imágenes podrían no mostrarse correctamente.")
                # --- <<< FIN: Sincronización de Imágenes >>> ---

                # --- Cargar definición de módulos ---
                self.update_message("Cargando definiciones de módulos...")
                start_load_modules_def = time.time()
                # Cargar módulos permitidos usando la función _load_allowed_modules
                Session.allowed_modules = self._load_allowed_modules(Session.user_data, self.user_alias)
                end_load_modules_def = time.time()
                print(f"Tiempo _load_allowed_modules (definición): {end_load_modules_def - start_load_modules_def:.4f} segundos")

                # --- Pre-Instanciar OverviewPage (Widget Inicial) ---
                self.update_message("Preparando interfaz principal...")
                Session.initial_widget_instance = None # Resetear
                inicio_definition = None
                inicio_index = -1
                # Buscar la definición del módulo "Inicio"
                for i, mod_def in enumerate(Session.allowed_modules):
                    if mod_def["name"] == "Inicio":
                        inicio_definition = mod_def
                        inicio_index = i
                        break

                # Si se encontró "Inicio", intentar pre-instanciarlo
                if inicio_definition:
                    print("Pre-instanciando widget para: Inicio")
                    factory = inicio_definition.get("widget_factory")
                    args = inicio_definition.get("widget_args", [])
                    if factory:
                        start_inst_inicio = time.time()
                        try:
                            # Crear la instancia del widget
                            initial_widget = factory(*args)
                            # Guardar la instancia y su índice en la sesión global
                            Session.initial_widget_instance = initial_widget
                            Session.initial_widget_index = inicio_index
                            end_inst_inicio = time.time()
                            print(f"TIMING (Login): Widget 'Inicio' pre-instanciado en {end_inst_inicio - start_inst_inicio:.4f} segundos.")
                        except Exception as e_inst:
                            print(f"Error al pre-instanciar 'Inicio': {e_inst}")
                            # No es crítico, se instanciará más tarde si falla aquí
                    else:
                        print("Error: No se encontró 'widget_factory' para 'Inicio'")
                else:
                    print("Advertencia: No se encontró la definición del módulo 'Inicio'.")
                # --- Fin Pre-Instanciar ---

                self.time_modules_processed = time.time() # Marcar tiempo final

                # --- Finalización del Login Exitoso ---
                self.login_button.setText("Continuar") # Cambiar texto del botón
                # Desconectar la acción inicial y conectar la acción de aceptar el diálogo
                try:
                    self.login_button.clicked.disconnect(self.start_login)
                except TypeError: # Ignorar si ya estaba desconectado
                    pass
                self.login_button.clicked.connect(self.accept) # Conectar a self.accept()
                self.login_button.setEnabled(True) # Habilitar el botón

                # Asegurar que la barra está al 100% y mostrar mensaje final
                self.progress_bar_total.setValue(100)
                self.update_message(f"Bienvenid@, {self.user_alias}")

                # Imprimir tiempo total del proceso de login
                total_time = time.time() - self.time_start_login
                print(f"Tiempo Total Login Dialog (con sync y pre-instancia): {total_time:.4f} s")

            else:
                # Si load_user_data_by_email devolvió None o False
                self.on_login_error("Usuario no autorizado o no encontrado en la base de datos.")

        except Exception as e:
            # Capturar cualquier error inesperado durante el proceso
            print(f"Error general en on_login_result: {e}")
            traceback.print_exc() # Imprimir traceback detallado para depuración
            self.on_login_error(f"Fallo inesperado al cargar perfil o sincronizar:\n{e}")


    def _load_allowed_modules(self, user_data, current_user_alias):
        """Define la estructura de los módulos permitidos y sus iconos."""
        print(f"Cargando definiciones de módulos para alias: {current_user_alias}")

        def get_icon_path(icon_filename):
            """Devuelve la ruta local del icono si existe, si no, la de red."""
            local_path = os.path.join(LOCAL_ICON_PATH, icon_filename)
            if os.path.exists(local_path):
                return local_path

            # Fallback a la ruta de red
            network_path = os.path.join(BASE_NETWORK_PATH, "Iconos_Botones", icon_filename)

            # Intentar copiar el icono si no existe localmente pero sí en red
            if not os.path.exists(local_path) and os.path.exists(network_path):
                try:
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    shutil.copy2(network_path, local_path)
                    print(f"Icono '{icon_filename}' copiado a '{local_path}'")
                    return local_path # Devolver ruta local después de copiar
                except Exception as e:
                    print(f"No se pudo copiar el icono '{icon_filename}': {e}")
                    return network_path # Devolver ruta de red si falla la copia
            elif not os.path.exists(local_path) and not os.path.exists(network_path):
                 # Si no existe en ninguna ubicación
                 print(f"Advertencia: Icono no encontrado ni local ni en red: {icon_filename}")
                 return "" # Devolver cadena vacía o ruta a un icono por defecto
            else:
                 # Existe en red pero no local (y no se pudo copiar o no se intentó)
                 return network_path

        # Definiciones de todos los módulos posibles
        modules_definitions = [
             {
                 "name": "Inventario", "desc": "Administración de inventario",
                 "widget_factory": Window2Page, "widget_args": [],
                 "icon": get_icon_path("Inventory_Bottom.ico"),
                 "permission": user_data.get("MODULE_1", "0") in ["1", "true", "True"] # Chequea permiso
             },
             {
                 "name": "Factor de Uso", "desc": "Cálculo del uso por ítem",
                 "widget_factory": Window3Page, "widget_args": [],
                 "icon": get_icon_path("FACTOR_DE_USO.png"),
                 "permission": user_data.get("MODULE_2", "0") in ["1", "true", "True"]
             },
             {
                 "name": "Mantenimiento", "desc": "Registro y control de mantenimiento...",
                 "widget_factory": Window4Page, "widget_args": [current_user_alias], # Pasa alias
                 "icon": get_icon_path("MANTENIMIENTO_HERRAMENTAL.ico"),
                 "permission": user_data.get("MODULE_3", "0") in ["1", "true", "True"]
             },
             {
                 "name": "Entrada/Salida", "desc": "Registro y control de ingreso/egreso...",
                 "widget_factory": Window5Page, "widget_args": [current_user_alias], # Pasa alias
                 "icon": get_icon_path("Entrada&Salida.png"),
                 "permission": user_data.get("MODULE_4", "0") in ["1", "true", "True"]
             },
             {
                 "name": "Control de fechas de expiración", "desc": "Monitoreo de caducidad...",
                 "widget_factory": Window6Page, "widget_args": [],
                 "icon": get_icon_path("Expiration.png"),
                 "permission": user_data.get("MODULE_5", "0") in ["1", "true", "True"]
             },
             {
                 "name": "Imprimir", "desc": "Impresión de etiquetas",
                 "widget_factory": Window7Page, "widget_args": [],
                 "icon": get_icon_path("Print.png"),
                 "permission": user_data.get("MODULE_6", "0") in ["1", "true", "True"]
             },
             {
                 "name": "Configuración y Personalización", "desc": "Preferencias y ajustes del usuario",
                 "widget_factory": Window8Page, "widget_args": [],
                 "icon": get_icon_path("Custom.png"),
                 "permission": user_data.get("MODULE_7", "0") in ["1", "true", "True"]
             },
             {
                 "name": "Historial", "desc": "Historial de movimientos del usuario",
                 "widget_factory": Window9Page, "widget_args": [],
                 "icon": get_icon_path("History.png"),
                 "permission": user_data.get("MODULE_8", "0") in ["1", "true", "True"]
             }
         ]

        # Siempre añadir "Inicio" como primer módulo, siempre permitido
        allowed = [{
             "name": "Inicio", "desc": "Vista general y acceso a módulos",
             "widget_factory": OverviewPage, "widget_args": [], # Se llenarán después
             "icon": get_icon_path("Home_Bottom.ico"),
             "permission": True # Siempre permitido
         }] + [module for module in modules_definitions if module["permission"]] # Añadir el resto si tienen permiso

        # Configurar los argumentos para OverviewPage (pasar la lista de módulos permitidos)
        for module in allowed:
             if module["name"] == "Inicio":
                 # Pasar una COPIA de la lista 'allowed' para evitar referencias circulares
                 module["widget_args"] = [list(allowed)]

        return allowed # Devuelve la lista final de definiciones de módulos permitidos


    def on_login_error(self, error_message):
        """Maneja los errores ocurridos durante el proceso de login."""
        # Mostrar mensaje crítico al usuario
        QtWidgets.QMessageBox.critical(self, "Error de Verificación", error_message)
        # Restaurar estado inicial del botón y la barra
        self.login_button.setEnabled(True)
        self.login_button.setText("Verificar Usuario")
        self.progress_bar_total.setValue(0) # Resetear barra única
        self.progress_bar_total.setStyleSheet(self.progress_bar_total.styleSheet().replace("5a913c", "893c91")) # Restaurar color inicial si se cambió
        self.message_label.setText("TOOLTRACK+") # Restaurar mensaje original
        # No se llama a self.reject() para permitir reintentos, a menos que el error sea fatal.


    def get_user_alias(self):
        """Devuelve el alias del usuario logueado."""
        # Asegurarse de que el alias se obtuvo correctamente
        return self.user_alias

# ================================================================
# Función check_update_permission (sin cambios)
# ================================================================
def check_update_permission(user_alias):
    """
    Verifica si el usuario tiene permisos de actualización ('UPDATE_OBJECT' == 'YES')
    basado en el caché de datos de usuario cargado previamente.
    """
    global USER_DATA_CACHE
    if USER_DATA_CACHE is None:
        print("Advertencia: Intentando verificar permisos antes de cargar caché de usuarios.")
        # Intenta cargar si no existe (puede ralentizar la primera vez que se llama si el login falló)
        # Opcional: Podrías intentar obtener el email del alias si tienes esa relación
        # Por ahora, si no hay caché, no hay permisos.
        return False

    # Buscar el usuario por ALIAS en el caché (requiere iterar si no está indexado por alias)
    user_info = None
    for email, data in USER_DATA_CACHE.items():
        if data.get("ALIAS") == user_alias:
            user_info = data
            break

    if user_info:
        permission = user_info.get("UPDATE_OBJECT", "NO") # Default a NO si no existe
        print(f"Verificando permiso UPDATE_OBJECT para {user_alias}: {permission}")
        # Asegurarse de comparar con 'YES' (o como esté definido en tu CSV)
        return permission.strip().upper() == "YES"
    else:
        print(f"No se encontraron datos en caché para el alias: {user_alias}")
        return False

GENERAL_STYLESHEET = """
    QWidget {
        font-family: Segoe UI, Arial, sans-serif;
        font-size: 10pt;
    }
    QLabel {
        color: #333;
        padding: 2px; /* Añadir un poco de padding a los labels */
    }
    QLineEdit, QComboBox, QListWidget, QTextEdit, QDateEdit { /* Añadir QDateEdit */
        border: 1px solid #ccc;
        padding: 4px;
        border-radius: 3px;
        background-color: white;
        min-height: 20px; /* Altura mínima consistente */
    }
    QLineEdit:focus, QComboBox:focus, QDateEdit:focus {
        border: 1px solid #0078D7;
    }
    QPushButton {
        background-color: #E1E1E1;
        color: black;
        border: 1px solid #ADADAD;
        padding: 5px 15px;
        border-radius: 3px;
        min-height: 22px; /* Altura mínima botones */
    }
    QPushButton:hover {
        background-color: #E5F1FB;
        border: 1px solid #0078D7;
    }
    QPushButton:pressed {
        background-color: #CCE4F7;
    }
    QPushButton:disabled {
        background-color: #F4F4F4;
        color: #ADADAD;
        border-color: #D1D1D1;
    }
    QGroupBox {
        border: 1px solid #ccc;
        border-radius: 4px;
        margin-top: 10px;
        padding-top: 10px;
        background-color: #f9f9f9;
    }
    QGroupBox::title {
        subcontrol-origin: margin;
        subcontrol-position: top left;
        padding: 0 5px 0 5px;
        left: 10px;
        color: #333;
        font-weight: bold;
    }
    QMessageBox QPushButton { /* Estilo específico para botones de QMessageBox */
        min-width: 80px; /* Ancho mínimo para botones Sí/No/Cancelar etc. */
        font-weight: bold;
        color: black; /* Asegurar texto negro por defecto */
    }
    QTableWidget {
        border: 1px solid #ccc;
        gridline-color: #e0e0e0; /* Color de las líneas de la cuadrícula */
        alternate-background-color: #f7f7f7; /* Color alterno para filas */
    }
    QTableWidget::item {
        padding: 3px;
    }
    QHeaderView::section {
        background-color: #f0f0f0;
        padding: 4px;
        border: 1px solid #ccc;
        font-weight: bold;
    }
"""

BTN_STYLE_ACCEPT = """
    QPushButton {
        background-color: #28a745; /* Verde */
        color: white;
        font-weight: bold;
        border: 1px solid #1e7e34;
        padding: 5px 15px;
        border-radius: 3px;
        min-height: 22px;
    }
    QPushButton:hover { background-color: #218838; border-color: #1c7e34; }
    QPushButton:pressed { background-color: #1e7e34; }
    QPushButton:disabled { background-color: #a3d3a3; color: #f0f0f0; border-color: #8dba8d; }
"""
# Definir un estilo para el botón de edición
BTN_STYLE_EDIT = """
    QPushButton {
        background-color: #007bff; /* Azul */
        color: white;
        font-weight: bold;
        border: 1px solid #0069d9;
        padding: 5px 15px;
        border-radius: 3px;
        min-height: 22px;
    }
    QPushButton:hover { background-color: #0069d9; border-color: #005cbf; }
    QPushButton:pressed { background-color: #005cbf; }
    QPushButton:disabled { background-color: #99c0e6; color: #f0f0f0; border-color: #80a9d5; }
"""

BTN_STYLE_REJECT = """
    QPushButton {
        background-color: #dc3545; /* Rojo para reject */
        color: white;
        font-weight: bold;
        border: 1px solid #c82333;
        padding: 5px 15px;
        border-radius: 3px;
        min-height: 22px;
    }
    QPushButton:hover { background-color: #c82333; border-color: #bd2130; }
    QPushButton:pressed { background-color: #bd2130; }
    QPushButton:disabled { background-color: #e79b9c; color: #f0f0f0; border-color: #d45d62; }
"""

BTN_STYLE_DANGER = """
    QPushButton {
        background-color: #ffc107; /* Amarillo para danger */
        color: black;
        font-weight: bold;
        border: 1px solid #e0a800;
        padding: 5px 15px;
        border-radius: 3px;
        min-height: 22px;
    }
    QPushButton:hover { background-color: #e0a800; border-color: #d39e00; }
    QPushButton:pressed { background-color: #d39e00; }
    QPushButton:disabled { background-color: #ffe08a; color: #f0f0f0; border-color: #dbc17f; }
"""

# Funciones globales (colócalas a nivel de módulo)
def adjust_color(hex_color, amount):
    rgb = [int(hex_color[i:i+2], 16) for i in (1, 3, 5)]
    new_rgb = [min(255, max(0, c + amount)) for c in rgb]
    return f"#{new_rgb[0]:02x}{new_rgb[1]:02x}{new_rgb[2]:02x}"

def generate_button_style(header_color):
    hover_color = adjust_color(header_color, 20)
    pressed_color = adjust_color(header_color, -20)
    style_text = f"""
    QPushButton {{
        background-color: {header_color};
        color: white;
        border: none;
        border-radius: 1px;
        padding: 1px 1px;
        font: bold 10pt 'Montserrat';
    }}
    QPushButton:hover {{
        background-color: {hover_color};
    }}
    QPushButton:pressed {{
        background-color: {pressed_color};
    }}
    """
    return style_text

# -----------------------------------------------------------------------------
# Función para registrar en el historial (versión robusta para columnas variables)
# -----------------------------------------------------------------------------
def write_history(user, nomenclatura, job="", linea="", user_mfg="", estado="", movimiento="", comentario="", qty=""):
    """
    Registra una entrada en el archivo CSV de historial.
    Maneja archivos con columnas potencialmente variables escritas por otros módulos.
    Adapta la estructura en memoria para su procesamiento sin sobrescribir el archivo.
    Realiza una comprobación de duplicados basada en campos clave y un umbral de tiempo.
    """
    try:
        # Usamos formato que incluye segundos para distinguir acciones rápidas
        now_dt = datetime.now()
        now_str = now_dt.strftime("%d/%m/%y %H:%M:%S") # Formato día/mes/año-2-dígitos hora:minuto:segundo

        # Define las columnas que esta función espera y escribirá.
        # Otros módulos podrían usar un subconjunto o tener columnas adicionales.
        expected_columns = [
            "History_ID", "USER", "NOMENCLATURA", "JOB", "LINEA",
            "USER MFG", "ESTADO", "MOVIMIENTO", "COMENTARIO", "QTY", "DATE"
        ]
        # Columnas clave para la verificación de duplicados
        required_check_cols = ["USER", "NOMENCLATURA", "JOB", "MOVIMIENTO", "QTY", "DATE"]

        # Se define el archivo de bloqueo basado en el HISTORY_PATH
        lock_path = HISTORY_PATH + ".lock"
        lock = FileLock(lock_path, timeout=10)  # Timeout en 10 segundos si no se consigue el lock

        with lock:
            file_exists = os.path.isfile(HISTORY_PATH)
            history_id = 1
            df_hist = pd.DataFrame(columns=expected_columns) # DataFrame por defecto vacío

            if file_exists:
                # Leer el archivo usando un único encoding y tratando todo como string inicialmente.
                # 'warn' notificará sobre líneas malformadas sin detenerse.
                try:
                    df_hist = pd.read_csv(
                        HISTORY_PATH,
                        encoding="utf-8-sig",
                        on_bad_lines='warn',
                        dtype=str # Leer todo como string para evitar inferencia errónea
                    )
                    if df_hist.empty and os.path.getsize(HISTORY_PATH) > 0:
                         print(f"Advertencia: Archivo de historial {HISTORY_PATH} leído como vacío, pero no tiene tamaño 0. Puede estar corrupto.")
                         # Tratar como si no existiera para reescribir encabezados si es necesario
                         file_exists = False


                except pd.errors.EmptyDataError:
                    print(f"Advertencia: Archivo de historial {HISTORY_PATH} está vacío. Se tratará como nuevo.")
                    df_hist = pd.DataFrame(columns=expected_columns)
                    file_exists = False # Tratar como si no existiera para escribir encabezados
                except Exception as read_err:
                    print(f"Error crítico al leer historial {HISTORY_PATH}: {read_err}")
                    # Considerar no continuar si la lectura falla catastróficamente
                    # QtWidgets.QMessageBox.critical(None, "Error Lectura Historial", f"Error crítico al leer historial:\n{read_err}")
                    return False

            # --- Adaptación de columnas en memoria (SIN SOBRESCRIBIR ARCHIVO) ---
            # Si las columnas leídas no coinciden exactamente con las esperadas,
            # se adapta el DataFrame *en memoria* para el procesamiento interno.
            current_columns = list(df_hist.columns)
            if current_columns != expected_columns:
                print("Advertencia: Encabezados del historial no coinciden con los esperados. Adaptando en memoria...")
                temp_df = pd.DataFrame(columns=expected_columns) # Crear df temporal con estructura ideal

                # Copiar columnas existentes que coinciden
                for col in expected_columns:
                    if col in df_hist.columns:
                        temp_df[col] = df_hist[col]
                    else:
                        temp_df[col] = "" # Añadir columna faltante vacía

                df_hist = temp_df # Reemplazar df_hist con la versión adaptada en memoria
                print("DataFrame adaptado en memoria con columnas:", list(df_hist.columns))
                # NO se guarda df_hist.to_csv aquí para no alterar el archivo original

            # --- Cálculo de History_ID ---
            if not df_hist.empty and "History_ID" in df_hist.columns:
                # Convertir History_ID a numérico para calcular el consecutivo.
                # Se usa una copia para evitar SettingWithCopyWarning si df_hist es una vista
                df_hist_copy = df_hist.copy()
                df_hist_copy["History_ID_num"] = pd.to_numeric(df_hist_copy["History_ID"], errors="coerce")
                valid_ids = df_hist_copy["History_ID_num"].dropna()
                if not valid_ids.empty:
                    try:
                        history_id = int(valid_ids.max() + 1)
                    except ValueError:
                        print("Advertencia: No se pudo calcular el ID máximo, usando 1.")
                        history_id = 1
                del df_hist_copy # Liberar memoria

            # --- Comprobación de Duplicados ---
            duplicate = False
            # Verificar si tenemos las columnas necesarias para el chequeo en el df adaptado
            if all(col in df_hist.columns for col in required_check_cols):
                # Parsear la columna DATE *solo* con el formato específico que esta función escribe
                # Se crea una columna temporal DATE_dt para la comparación
                try:
                    # Crear copia para evitar SettingWithCopyWarning
                    df_check = df_hist.copy()
                    # Asegurar que la columna DATE existe antes de intentar parsearla
                    if 'DATE' in df_check.columns:
                        df_check['DATE_dt'] = pd.to_datetime(
                            df_check['DATE'],
                            format="%d/%m/%y %H:%M:%S", # Formato específico de esta función
                            errors='coerce' # Fechas con otro formato serán NaT
                        )

                        # Filtrar filas que coincidan en los campos clave (comparando como strings)
                        # Asegurarse de que los argumentos de la función también se traten como strings
                        str_user = str(user)
                        str_nomenclatura = str(nomenclatura)
                        str_job = str(job)
                        str_movimiento = str(movimiento)
                        str_qty = str(qty)

                        # Construir filtro booleano
                        match_filter = (
                            (df_check["USER"].astype(str) == str_user) &
                            (df_check["NOMENCLATURA"].astype(str) == str_nomenclatura) &
                            (df_check["JOB"].astype(str) == str_job) &
                            (df_check["MOVIMIENTO"].astype(str) == str_movimiento) &
                            (df_check["QTY"].astype(str) == str_qty) &
                            (pd.notna(df_check['DATE_dt'])) # Solo considerar filas con fecha parseada correctamente
                        )
                        matches = df_check[match_filter]

                        # Iterar sobre las coincidencias potenciales
                        for _, row in matches.iterrows():
                            # row['DATE_dt'] ya es un objeto datetime si no es NaT
                            diff = (now_dt - row['DATE_dt']).total_seconds()
                            # Considerar duplicado si la acción es casi simultánea (menos de 10 seg)
                            if 0 <= diff < 10:
                                duplicate = True
                                break
                        del df_check # Liberar memoria

                    else:
                         print("Advertencia: La columna 'DATE' no existe en el historial leído. No se puede realizar chequeo de duplicados basado en fecha.")

                except Exception as e:
                    print(f"Error durante la comprobación de duplicados: {e}")
                    # Podría decidirse continuar sin la comprobación o retornar False

            else:
                missing_req_cols = [col for col in required_check_cols if col not in df_hist.columns]
                print(f"Advertencia: Faltan columnas esenciales ({missing_req_cols}) para la verificación completa de duplicados en el historial.")

            # Si se detecta un duplicado, informar y salir
            if duplicate:
                print("Acción duplicada detectada recientemente. No se registrará de nuevo.")
                # QtWidgets.QMessageBox.information(None, "Información", "La acción ya fue realizada previamente.") # UI Comentada
                return False # Indicar que no se escribió por ser duplicado

            # --- Escritura en el archivo CSV ---
            # Abre el archivo (creándolo si es necesario) para agregar la nueva entrada.
            # Usar 'a' para append. newline='' es importante para csv.writer.
            with open(HISTORY_PATH, mode='a', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                # Escribir encabezados solo si el archivo NO existía O si existía pero estaba vacío/malformado
                # Se verifica df_hist.empty por si la lectura inicial resultó en un DF vacío
                # y file_exists era True (ej. archivo corrupto).
                write_headers = not file_exists or (file_exists and df_hist.empty)

                if write_headers:
                    writer.writerow(expected_columns)
                    print("Escribiendo encabezados en el archivo de historial.")

                # Escribir la nueva fila, asegurando que todos los datos sean strings
                writer.writerow([
                    str(history_id),
                    str(user),
                    str(nomenclatura),
                    str(job),
                    str(linea),
                    str(user_mfg),
                    str(estado),
                    str(movimiento),
                    str(comentario),
                    str(qty),
                    now_str # La fecha ya está formateada como string
                ])
            print(f"[LOG] Registro agregado: ID={history_id}, User={user}, Nomen={nomenclatura}, Mov={movimiento}, Date={now_str}")
            return True # Indicar éxito

    except FileLock.Timeout:
        print(f"Error: No se pudo obtener el bloqueo para el archivo de historial '{HISTORY_PATH}' después de 10 segundos.")
        # QtWidgets.QMessageBox.critical(None, "Error de Bloqueo", f"No se pudo acceder al archivo de historial:\n{HISTORY_PATH}\n\nOtro proceso podría estar usándolo.")
        return False
    except Exception as e:
        print(f"Error general al escribir en el historial: {e}")
        import traceback
        traceback.print_exc() # Imprime el traceback completo para depuración
        # QtWidgets.QMessageBox.critical(None, "Error", f"Error al escribir en el historial:\n{e}") # UI Comentada
        return False

# ================================================================
# Funciones auxiliares para efectos de sombra en los botones de la title bar
# ================================================================
def add_shadow_effect(button):
    shadow = QtWidgets.QGraphicsDropShadowEffect()
    shadow.setBlurRadius(5)
    shadow.setOffset(2, 2)
    shadow.setColor(QtGui.QColor(0, 0, 0, 80))
    button.setGraphicsEffect(shadow)
    button._shadow = shadow

def _set_shadow_pressed(button):
    if hasattr(button, '_shadow'):
        button._shadow.setOffset(0, 0)
        button._shadow.setBlurRadius(2)

def _set_shadow_released(button):
    if hasattr(button, '_shadow'):
        button._shadow.setOffset(2, 2)
        button._shadow.setBlurRadius(5)

# Estilos Globales (# Botón cerrarjemplo de variables de estilo para botones)
STYLE_BUTTON = """
    QPushButton {
        background-color: #d99227;
        color: white;
        border: none;
        border-radius: 1px;
        padding: 1px 1px;
        font: bold 10pt 'Montserrat';
    }
    QPushButton:hover {
        background-color: #e6a33a;
    }
    QPushButton:pressed {
        background-color: #a66d2d;
    }
"""

    

class PlaceholderWidget(QtWidgets.QWidget):
     def __init__(self, name="Placeholder", alias=None):
         super().__init__()
         layout = QtWidgets.QVBoxLayout(self)
         label_text = f"Widget para: {name}"
         if alias:
             label_text += f"\nUsuario: {alias}"
         self.label = QtWidgets.QLabel(label_text)
         self.label.setAlignment(QtCore.Qt.AlignCenter)
         layout.addWidget(self.label)
         print(f"Widget '{name}' instanciado.")
# --------------------------------------------------------------------

# --- Funciones Auxiliares ---
from datetime import datetime, timedelta
import pandas as pd

def parse_date(date_str, formats=['%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S']):
    """Intenta parsear una fecha desde varios formatos comunes."""
    if pd.isna(date_str) or not isinstance(date_str, str) or not date_str.strip():
        return None
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except (ValueError, TypeError):
            continue
    print(f"Advertencia: No se pudo parsear la fecha '{date_str}' con los formatos {formats}")
    return None  # O lanzar un error si prefieres

def format_date(dt_obj):
    """Formatea un objeto datetime a dd/mm/yyyy."""
    if isinstance(dt_obj, datetime):
        return dt_obj.strftime('%d/%m/%Y')
    return ""

def get_week_start(dt_obj):
    """Obtiene el lunes de la semana de una fecha dada."""
    if isinstance(dt_obj, datetime):
        return dt_obj - timedelta(days=dt_obj.weekday())
    return None

# ================================================================
# Diálogo para Modificar Fecha de Llegada
# ================================================================
class ModifyDateDialog(QtWidgets.QDialog):
    def __init__(self, current_date=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Modificar Fecha de Llegada")
        layout = QtWidgets.QVBoxLayout(self)

        self.calendar = QtWidgets.QCalendarWidget(self)
        self.calendar.setGridVisible(True)
        
        if isinstance(current_date, datetime):
            # Fija la fecha recibida usando QDate
            self.calendar.setSelectedDate(QDate(current_date.year, current_date.month, current_date.day))
        else:
            # Por defecto, se establece la fecha de hoy
            self.calendar.setSelectedDate(QDate.currentDate())

        layout.addWidget(self.calendar)

        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def getSelectedDate(self):
        qdate = self.calendar.selectedDate()
        # Convertir QDate a datetime usando solo año, mes y día
        return datetime(qdate.year(), qdate.month(), qdate.day())
    
class MultiRowHeader(QtWidgets.QHeaderView):
    """
    Encabezado horizontal personalizado con dos filas,
    en el que la sección inferior se divide en dos líneas:
      - La línea superior muestra el "stock actual" de la semana.
      - La línea inferior muestra el "stock final" de la semana.
    Para la columna 0 se muestra la etiqueta "Stock general".
    """
    def __init__(self, orientation, parent=None):
        super().__init__(orientation, parent)
        self.global_current = []   # Lista con el stock actual para columnas 1..n
        self.global_final = []     # Lista con el stock final para columnas 1..n
        self.setFixedHeight(80)    # Altura suficiente para dos líneas en el subencabezado

    def setGlobalValues(self, current_values, final_values):
        """
        Recibe dos listas: 
          - current_values: stock actual de cada semana (columnas 1..n)
          - final_values: stock final (stock actual menos demanda) para cada semana.
        """
        self.global_current = current_values
        self.global_final = final_values
        self.update()

    def paintSection(self, painter, rect, logicalIndex):
        if not rect.isValid():
            super().paintSection(painter, rect, logicalIndex)
            return

        painter.save()

        # Fondo con degradado para la sección completa
        gradient = QtGui.QLinearGradient(rect.topLeft(), rect.bottomRight())
        gradient.setColorAt(0, QtGui.QColor("#3aafa9"))
        gradient.setColorAt(1, QtGui.QColor("#276678"))
        painter.fillRect(rect, gradient)

        # Dividir el rectángulo en dos franjas:
        #  - top_rect: para el título original de la columna.
        #  - bottom_rect: para el subencabezado.
        top_rect = QtCore.QRect(rect)
        bottom_rect = QtCore.QRect(rect)
        top_rect.setBottom(rect.top() + int(rect.height() * 0.6))
        bottom_rect.setTop(top_rect.bottom())

        # Dibujar la fila superior (título original)
        header_text = str(self.model().headerData(logicalIndex, self.orientation(), QtCore.Qt.DisplayRole))
        painter.setPen(QtCore.Qt.white)
        painter.drawText(top_rect, QtCore.Qt.AlignCenter | QtCore.Qt.TextWordWrap, header_text)

        # Para la columna 0, se muestra "Stock general" en la sección inferior.
        if logicalIndex == 0:
            adjusted_rect = bottom_rect.adjusted(5, 0, -5, 0)
            painter.drawText(adjusted_rect, QtCore.Qt.AlignCenter | QtCore.Qt.TextWordWrap | QtCore.Qt.AlignVCenter, "Stock general")
        else:
            # Para las demás columnas, se deben mostrar dos secciones.
            # Dividimos bottom_rect en dos subrectángulos iguales verticalmente.
            bottom_top_rect = QtCore.QRect(bottom_rect)
            bottom_top_rect.setBottom(bottom_rect.top() + int(bottom_rect.height() / 2))
            bottom_bottom_rect = QtCore.QRect(bottom_rect)
            bottom_bottom_rect.setTop(bottom_rect.top() + int(bottom_rect.height() / 2))

            index = logicalIndex - 1  # porque global_values corresponde a columnas desde la 1.
            if index < len(self.global_current):
                current_text = str(self.global_current[index])
            else:
                current_text = ""
            if index < len(self.global_final):
                final_text = str(self.global_final[index])
            else:
                final_text = ""

            # Determinar el color de fondo para la sección inferior,
            # basado en el stock final: si es <= 0 se pinta en rojo; de lo contrario, en verde claro.
            try:
                final_value = float(final_text)
            except Exception:
                final_value = 0
            if final_value <= 0:
                bg_color = QtGui.QColor("red")
            else:
                bg_color = QtGui.QColor("#90EE90")

            # Dibujar un rectángulo redondeado en el área destinada al stock final (bottom_bottom_rect)
            radius = 5
            adjusted_bottom = bottom_bottom_rect.adjusted(2, 2, -2, -2)
            path = QtGui.QPainterPath()
            path.addRoundedRect(QtCore.QRectF(adjusted_bottom), radius, radius)
            painter.fillPath(path, bg_color)

            # Dibujar el stock actual en la parte superior del subencabezado...
            painter.setPen(QtCore.Qt.black)
            painter.drawText(bottom_top_rect, QtCore.Qt.AlignCenter | QtCore.Qt.TextWordWrap, current_text)
            # ...y el stock final en la parte inferior.
            painter.drawText(bottom_bottom_rect, QtCore.Qt.AlignCenter | QtCore.Qt.TextWordWrap, final_text)

        painter.restore()

class TimelineItemDelegate(QtWidgets.QStyledItemDelegate):
    """
    Delegate para pintar celdas de la línea de tiempo con fondo degradado,
    bordes redondeados y ajuste dinámico de tamaño según el contenido del texto.
    
    La asignación de color se maneja de la siguiente forma:
      - Si el valor es vacío o comienza con "Llega" (por ejemplo, "Llega 29/05/2025"):
          Se pinta con un color neutro (#f0f0f0) y se fuerza a que no se muestre texto.
      - Si el valor contiene "Corto": se pinta en rojo (#ff6e6e) y se muestra el texto en blanco.
      - Si el valor contiene "Expirado": se pinta en amarillo y se muestra el texto en negro.
      - Caso contrario: se asigna un color único basado en la fila (Arrival date)
          usando una paleta predefinida, y se muestra el texto en blanco.
    """
    def getColorForRow(self, row):
        # Paleta de colores para Arrival dates (excluyendo tonos rojos y amarillos)
        palette = ["#76c7c0", "#8ecae6", "#219ebc", "#023047", "#f15bb5", "#6a040f", "#007f5f", "#2b9348"]
        return QtGui.QColor(palette[row % len(palette)])
    
    def paint(self, painter, option, index):
        value = index.data(QtCore.Qt.DisplayRole)
        value_str = str(value) if value is not None else ""
        
        if value_str == "" or value_str.startswith("Llega"):
            bg_color = QtGui.QColor("#f0f0f0")
            text_color = QtCore.Qt.black
            value_str = ""
        elif "Corto" in value_str:
            bg_color = QtGui.QColor("#ff6e6e")
            text_color = QtCore.Qt.white
        elif "Expirado" in value_str:
            bg_color = QtGui.QColor("yellow")
            text_color = QtCore.Qt.black
        else:
            bg_color = self.getColorForRow(index.row())
            text_color = QtCore.Qt.white
        
        painter.save()

        # Configurar la fuente con un tamaño adecuado
        font = option.font
        font.setPointSize(10)
        painter.setFont(font)

        # Ajustar el rectángulo de dibujo con un margen interno
        rect = option.rect.adjusted(2, 2, -2, -2)
        radius = 5

        # Crear un QPainterPath con el rectángulo redondeado
        path = QtGui.QPainterPath()
        path.addRoundedRect(QtCore.QRectF(rect), radius, radius)

        # Degradado para el fondo
        grad = QtGui.QLinearGradient(rect.topLeft(), rect.bottomRight())
        grad.setColorAt(0, bg_color.lighter(110))
        grad.setColorAt(1, bg_color.darker(110))
        painter.fillPath(path, grad)

        painter.setPen(QtGui.QPen(text_color))
        # Dibujar el texto con word wrap para que se ajuste al área de la celda
        painter.drawText(rect, QtCore.Qt.AlignCenter | QtCore.Qt.TextWordWrap, value_str)
        
        painter.restore()
    
    def sizeHint(self, option, index):
        value = index.data(QtCore.Qt.DisplayRole)
        value_str = str(value) if value is not None else ""
        fm = QtGui.QFontMetrics(option.font)
        text_rect = fm.boundingRect(QtCore.QRect(0, 0, 300, 0), QtCore.Qt.TextWordWrap, value_str)
        width = text_rect.width() + 10
        height = text_rect.height() + 10
        return QtCore.QSize(width, height)


class CaducidadPOTab(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Dashboard Caducidad PO")
        self.po_df = pd.DataFrame()           # Datos de POs (llegadas)
        self.inventory_df = pd.DataFrame()    # Datos de inventario/demanda
        self.current_item_data = pd.DataFrame()  # Filtro por ítem seleccionado
        self.current_inventory_data = pd.Series(dtype=object)
        self.demand_cols = []  # Lista de tuplas: (fecha_demanda, nombre_columna)
        # Variables para agrupamiento de columnas de demandas (semanas pasadas)
        self.grouped_columns = []  # Índices de columnas (en la tabla) a agrupar
        self.groupButton = None    # Botón para alternar agrupamiento
        self.initUI()
        self.load_data()

    def initUI(self):
        main_layout = QtWidgets.QVBoxLayout(self)

        # --- Controles superiores ---
        top_layout = QtWidgets.QHBoxLayout()
        top_layout.addWidget(QtWidgets.QLabel("Seleccionar Item:"))
        self.item_combo = QtWidgets.QComboBox()
        self.item_combo.setMinimumWidth(250)
        self.item_combo.currentIndexChanged.connect(self.on_item_selected)
        top_layout.addWidget(self.item_combo)

        self.refresh_button = QtWidgets.QPushButton("Refrescar Datos")
        self.refresh_button.clicked.connect(self.load_data)
        top_layout.addWidget(self.refresh_button)

        self.confirm_arrivals_button = QtWidgets.QPushButton("Confirmar Llegadas Hoy")
        self.confirm_arrivals_button.clicked.connect(self.confirm_arrivals)
        top_layout.addWidget(self.confirm_arrivals_button)
        
        # NUEVO: Botón para administrar el inventario físico
        self.manage_inventory_button = QtWidgets.QPushButton("Administrar inventario físico")
        self.manage_inventory_button.clicked.connect(self.manage_inventory)
        top_layout.addWidget(self.manage_inventory_button)
        
        top_layout.addStretch(1)
        main_layout.addLayout(top_layout)

        # --- Dashboard ---
        self.dashboard_table = QtWidgets.QTableWidget()
        self.dashboard_table.setShowGrid(True)
        self.dashboard_table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.dashboard_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.dashboard_table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.dashboard_table.cellDoubleClicked.connect(self.on_cell_double_clicked)

        self.dashboard_table.setStyleSheet("""
            QTableWidget {
                font-family: "Segoe UI", Arial, sans-serif;
                font-size: 10pt;
                background-color: #fcfcfc;
                gridline-color: #dcdcdc;
            }
            QTableWidget::item {
                padding: 4px;
            }
        """)
        self.dashboard_table.setAlternatingRowColors(True)

        # Delegate y header personalizados
        self.dashboard_table.setItemDelegate(TimelineItemDelegate())
        custom_header = MultiRowHeader(QtCore.Qt.Horizontal, self.dashboard_table)
        self.dashboard_table.setHorizontalHeader(custom_header)
        self.dashboard_table.horizontalHeader().setDefaultAlignment(QtCore.Qt.AlignCenter)
        self.dashboard_table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        if hasattr(self.dashboard_table.horizontalHeader(), "setWordWrap"):
            self.dashboard_table.horizontalHeader().setWordWrap(True)

        # Configurar header vertical
        self.dashboard_table.verticalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        if hasattr(self.dashboard_table.verticalHeader(), "setWordWrap"):
            self.dashboard_table.verticalHeader().setWordWrap(True)
        self.dashboard_table.verticalHeader().setStyleSheet("""
            QHeaderView::section {
                background-color: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #276678, stop:1 #3aafa9);
                color: white;
                padding: 8px;
                border: 1px solid #aaaaaa;
                font-size: 12pt;
                font-weight: bold;
                border-radius: 4px;
            }
        """)
        shadow = QtWidgets.QGraphicsDropShadowEffect()
        shadow.setBlurRadius(12)
        shadow.setOffset(2, 2)
        shadow.setColor(QtGui.QColor(0, 0, 0, 80))
        self.dashboard_table.setGraphicsEffect(shadow)
        
        main_layout.addWidget(self.dashboard_table)


    def load_data(self):
        """Carga datos desde archivos CSV (POs e inventario/demanda) y actualiza el combobox."""
        print("Cargando datos para Dashboard Caducidad PO...")

        # --- Cargar POs (llegadas) ---
        if not os.path.exists(DASHBOARD_PATH):
            QtWidgets.QMessageBox.warning(self, "Archivo no encontrado",
                f"No se encontró el archivo:\n{DASHBOARD_PATH}")
            self.po_df = pd.DataFrame()
        else:
            try:
                self.po_df = pd.read_csv(DASHBOARD_PATH, encoding="utf-8-sig")
                self.po_df['Arrive Date'] = self.po_df['Arrive Date'].apply(parse_date)
                self.po_df['Expirate Date'] = self.po_df['Expirate Date'].apply(parse_date)
                self.po_df['QTY'] = pd.to_numeric(self.po_df['QTY'], errors='coerce').fillna(0).astype(int)
                self.po_df['Expirate Days'] = pd.to_numeric(self.po_df['Expirate Days'], errors='coerce').fillna(0).astype(int)
                # Asegurarse de limpiar espacios en blanco en la columna Item
                self.po_df['Item'] = self.po_df['Item'].astype(str).str.strip()
                self.po_df['STATUS'] = self.po_df['STATUS'].astype(str).fillna("DESCONOCIDO")
                self.po_df = self.po_df.sort_values(by=["Item", "Arrive Date"])
                print(f"POs cargados: {len(self.po_df)} registros.")
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error en POs",
                    f"Error al cargar las llegadas:\n{e}")
                traceback.print_exc()
                self.po_df = pd.DataFrame()

        # --- Cargar inventario y columnas de demanda ---
        if not os.path.exists(CONSUMABLE_INVENTORY_PATH):
            QtWidgets.QMessageBox.warning(self, "Archivo no encontrado",
                f"No se encontró el inventario:\n{CONSUMABLE_INVENTORY_PATH}")
            self.inventory_df = pd.DataFrame()
        else:
            try:
                self.inventory_df = pd.read_csv(CONSUMABLE_INVENTORY_PATH, encoding="utf-8-sig")
                # Limpieza de la columna Item
                self.inventory_df['Item'] = self.inventory_df['Item'].astype(str).str.strip()
                self.inventory_df['Qty OH'] = pd.to_numeric(self.inventory_df['Qty OH'], errors='coerce').fillna(0).astype(float)
                print(f"Inventario cargado: {len(self.inventory_df)} registros.")

                # Detectar y ordenar columnas de demanda (fechas)
                self.demand_cols = []
                for col in self.inventory_df.columns:
                    if re.match(r'\d{1,2}/\d{1,2}/\d{4}', col):
                        dt_obj = parse_date(col)
                        if dt_obj:
                            self.demand_cols.append((dt_obj, col))
                self.demand_cols.sort(key=lambda x: x[0])
                for dt_obj, col in self.demand_cols:
                    self.inventory_df[col] = pd.to_numeric(self.inventory_df[col], errors='coerce').fillna(0).astype(float)
                print("Columnas de demanda:", [col for _, col in self.demand_cols])
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error en Inventario",
                    f"Error al cargar el inventario:\n{e}")
                traceback.print_exc()
                self.inventory_df = pd.DataFrame()

        # --- Actualizar inventario basado en log de stock ---
        self.update_inventory_with_stock_log()

        # --- Actualizar combobox ---
        # Se extraen los ítems de ambos DataFrames y se hace la unión
        po_items = set()
        if not self.po_df.empty and "Item" in self.po_df.columns:
            po_items = set(self.po_df["Item"].astype(str).str.strip())
            print("DEBUG: Ítems obtenidos de POs:", po_items)
        inv_items = set()
        if not self.inventory_df.empty and "Item" in self.inventory_df.columns:
            inv_items = set(self.inventory_df["Item"].astype(str).str.strip())
            print("DEBUG: Ítems obtenidos de Inventario:", inv_items)
            
        unique_items = sorted(po_items.union(inv_items))
        print("DEBUG: Ítems únicos para el combobox:", unique_items)

        self.item_combo.blockSignals(True)
        self.item_combo.clear()
        if unique_items:
            self.item_combo.addItems(unique_items)
            self.item_combo.blockSignals(False)
            # Opcional: seleccionar directamente el ítem deseado, por ejemplo:
            index_to_select = self.item_combo.findText("FLX-CHE-CFM-N00124")
            if index_to_select != -1:
                self.item_combo.setCurrentIndex(index_to_select)
                print("DEBUG: Seleccionado 'FLX-CHE-CFM-N00124' en el combobox.")
            else:
                self.on_item_selected(0)
        else:
            self.item_combo.blockSignals(False)
            self.clear_dashboard()
    
    def update_inventory_with_stock_log(self):
        """
        Actualiza la columna "Qty OH" del DataFrame de inventario usando la información
        del log de stock (STOCK_LOG_PATH) para la semana de interés. Primero valida si hay
        discrepancia entre el valor actual de "Qty OH" y "StockBefore" del log, ya que para
        esa semana se asume que "StockBefore" representa el stock actual.
        """
        if not os.path.exists(STOCK_LOG_PATH):
            print(f"DEBUG: No se encontró el archivo de log de stock en {STOCK_LOG_PATH}")
            return

        try:
            # Cargar el log y convertir 'EventDate' a objeto datetime
            stock_log = pd.read_csv(STOCK_LOG_PATH, encoding="utf-8-sig")
            print(f"DEBUG: Log de stock cargado con {len(stock_log)} registros.")
            stock_log['EventDate'] = stock_log['EventDate'].apply(parse_date)
            
            # Definir la semana de interés: ejemplo, del 05/05/2025 al 11/05/2025.
            week_start = parse_date("05/05/2025")
            week_end = week_start + pd.Timedelta(days=6)
            print(f"DEBUG: Rango de fecha de la semana de interés: {week_start} hasta {week_end}")

            # Recorrer cada ítem en el inventario
            for item in self.inventory_df['Item'].unique():
                item_log = stock_log[stock_log['Item'] == item]
                weekly_log = item_log[(item_log['EventDate'] >= week_start) & (item_log['EventDate'] <= week_end)]
                
                if not weekly_log.empty:
                    last_record = weekly_log.sort_values(by="EventDate").iloc[-1]
                    expected_qty = last_record["StockBefore"]
                    current_qty = self.inventory_df.loc[self.inventory_df['Item'] == item, 'Qty OH'].iloc[0]
                    if current_qty != expected_qty:
                        print(f"DEBUG: Discrepancia detectada en {item}: Qty OH actual = {current_qty}, StockBefore = {expected_qty}. Se actualiza.")
                        self.inventory_df.loc[self.inventory_df['Item'] == item, 'Qty OH'] = expected_qty
                    else:
                        print(f"DEBUG: No hay discrepancia en {item}: Qty OH = {current_qty} es correcto.")
                else:
                    print(f"DEBUG: No se encontró registro en el log de stock para {item} en la semana indicada.")
            
            print("DEBUG: Finalizada la actualización de inventario con el stock log.")
        except Exception as e:
            print("DEBUG: Error al actualizar inventario con stock log:", e)
            QtWidgets.QMessageBox.critical(self, "Error en log de stock",
                f"Error al actualizar inventario basado en el log de stock:\n{e}")
            traceback.print_exc()


    def on_item_selected(self, index):
        """Filtra los datos según el ítem seleccionado y refresca el dashboard."""
        selected_item = self.item_combo.currentText().strip()
        print(f"DEBUG: Cambio en combobox. Índice actual: {index}, Ítem seleccionado: '{selected_item}'")
        
        # Validaciones iniciales
        if not selected_item:
            print("DEBUG: No se seleccionó ningún ítem en el combobox.")
            self.clear_dashboard()
            return
        if self.po_df.empty:
            print("DEBUG: El DataFrame po_df está vacío.")
            self.clear_dashboard()
            return
        if self.inventory_df.empty:
            print("DEBUG: El DataFrame inventory_df está vacío.")
            self.clear_dashboard()
            return

        # Filtrar registros de POs para el item seleccionado
        self.current_item_data = self.po_df[self.po_df['Item'] == selected_item].copy()
        print(f"DEBUG: Se encontraron {len(self.current_item_data)} registros en po_df para el ítem '{selected_item}'")
        
        # Filtrar el inventario para el item seleccionado
        inventory_row = self.inventory_df[self.inventory_df['Item'] == selected_item]
        print(f"DEBUG: Se encontraron {len(inventory_row)} registros en inventory_df para el ítem '{selected_item}'")
        
        # Validar existencia de datos
        if self.current_item_data.empty:
            print(f"DEBUG: No hay registros en po_df para '{selected_item}'.")
            QtWidgets.QMessageBox.information(self, "Sin datos", f"No hay registros para {selected_item}.")
            self.clear_dashboard()
            return
        if inventory_row.empty:
            print(f"DEBUG: No se encontró inventario para '{selected_item}'.")
            QtWidgets.QMessageBox.warning(self, "Sin inventario", f"No se halló inventario para {selected_item}.")
            self.clear_dashboard()
            return
        
        self.current_inventory_data = inventory_row.iloc[0]
        print("DEBUG: Datos de inventario para el ítem seleccionado:", self.current_inventory_data)
        
        self.populate_dashboard()

    def clear_dashboard(self):
        """Limpia el contenido de la tabla sin remover encabezados."""
        self.dashboard_table.clearContents()
        self.dashboard_table.setRowCount(0)


# ================================================================
# FUNCIÓN populate_dashboard CORREGIDA (v5 - Fix Header Calc v3)
# ================================================================
    def populate_dashboard(self)    :
        """
        Construye el dashboard (línea de tiempo estilo Gantt) simulando un consumo FIFO virtual.
        Prioriza el inventario físico actual y lotes antiguos antes de consumir lotes nuevos.
        Actualiza los valores del header global. Muestra "Agotado" solo en la semana que ocurre.
        """
        print("DEBUG: Iniciando populate_dashboard...") # DEBUG
        # --- 0. Limpiar la tabla ---
        try:
            self.dashboard_table.clearContents()
            self.dashboard_table.setRowCount(0)
            self.dashboard_table.setColumnCount(0) # Limpiar columnas también
        except Exception as e:
            print(f"DEBUG: Error limpiando tabla: {e}")
            return # Salir si hay error aquí

        selected_item = self.item_combo.currentText()
        print(f"DEBUG: Item seleccionado: {selected_item}") # DEBUG

        # Validaciones iniciales
        if not selected_item:
            print("DEBUG: No hay item seleccionado.")
            self.clear_dashboard()
            return
        if self.current_item_data is None or self.current_item_data.empty:
            print("DEBUG: current_item_data (POs filtrados) está vacío.")
            self.clear_dashboard()
            return
        if not isinstance(self.current_inventory_data, pd.Series) or self.current_inventory_data.empty:
            print("DEBUG: current_inventory_data no es válido o está vacío.")
            self.clear_dashboard()
            return

        print("DEBUG: Datos iniciales validados.") # DEBUG

        # --- 1. Preparar y ordenar los POs para el ítem ---
        try:
            arrivals = self.current_item_data.copy()
            # Las fechas ya son datetime desde load_data
            print(f"DEBUG: Arrivals antes de dropna: {len(arrivals)} filas") # DEBUG
            arrivals = arrivals.dropna(subset=['Arrive Date'])
            print(f"DEBUG: Arrivals después de dropna('Arrive Date'): {len(arrivals)} filas") # DEBUG
            if arrivals.empty:
                print("DEBUG: No hay llegadas válidas después de procesar fechas (posible error en load_data o datos originales).")
                self.clear_dashboard()
                return
            arrivals['QTY'] = pd.to_numeric(arrivals['QTY'], errors='coerce').fillna(0)
            arrivals['STATUS'] = arrivals['STATUS'].astype(str).fillna('').str.upper()
            arrivals = arrivals.sort_values(by='Arrive Date').reset_index(drop=True)
            print(f"DEBUG: Arrivals preparados y ordenados: {len(arrivals)} filas.") # DEBUG
        except Exception as e:
            print(f"DEBUG: Error preparando arrivals: {e}")
            traceback.print_exc()
            self.clear_dashboard()
            return

        # --- 2. Definir el rango temporal ---
        try:
            min_po_date = arrivals['Arrive Date'].min()
            if self.demand_cols:
                valid_demand_dates = [d[0] for d in self.demand_cols if isinstance(d[0], datetime)]
                if not valid_demand_dates:
                    min_demand_date = max_demand_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                    print("DEBUG: Advertencia: No se encontraron fechas de demanda válidas.")
                else:
                    min_demand_date = min(valid_demand_dates)
                    max_demand_date = max(valid_demand_dates)
                min_date = min(min_po_date if pd.notna(min_po_date) else min_demand_date, min_demand_date)
            else:
                min_date = min_po_date if pd.notna(min_po_date) else datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                max_demand_date = min_date
                print("DEBUG: Advertencia: No hay columnas de demanda definidas.")

            initial_stock_physical_fallback = float(self.current_inventory_data.get('Qty OH', 0))
            print(f"DEBUG: Stock físico (Qty OH) leído como fallback: {initial_stock_physical_fallback}") # DEBUG
            valid_expiry_dates = arrivals['Expirate Date'].dropna()
            max_expiry_date = valid_expiry_dates.max() if not valid_expiry_dates.empty else datetime.now()
            max_po_date = arrivals['Arrive Date'].max()
            comparable_dates = [d for d in [max_demand_date, max_expiry_date, max_po_date, datetime.now()] if pd.notna(d)]
            max_date = max(comparable_dates) + timedelta(weeks=12) if comparable_dates else datetime.now() + timedelta(weeks=52)
            print(f"DEBUG: Rango temporal: {format_date(min_date)} a {format_date(max_date)}") # DEBUG
        except Exception as e:
            print(f"DEBUG: Error definiendo rango temporal o stock inicial: {e}"); traceback.print_exc(); self.clear_dashboard(); return

        # --- 3. Generar encabezados semanales ---
        try:
            self.week_headers = []
            if pd.isna(min_date) or not isinstance(min_date, datetime): print("DEBUG: Error: Fecha de inicio inválida para generar headers."); return
            current_week_header_dt = get_week_start(min_date)
            if current_week_header_dt is None: print("DEBUG: Error: No se pudo calcular la semana de inicio para headers."); return
            loop_count = 0; max_loops = 520
            while current_week_header_dt <= max_date and loop_count < max_loops:
                self.week_headers.append(current_week_header_dt)
                next_week_candidate = current_week_header_dt + timedelta(weeks=1)
                if pd.isna(next_week_candidate): print(f"DEBUG: Error: Cálculo de siguiente semana inválido desde {current_week_header_dt}"); break
                current_week_header_dt = next_week_candidate; loop_count += 1
            if loop_count >= max_loops: print("DEBUG: Advertencia: Límite de bucle de encabezados alcanzado.")
            if not self.week_headers: print("DEBUG: Error: No se generaron encabezados semanales."); return
            print(f"DEBUG: Generados {len(self.week_headers)} encabezados semanales.") # DEBUG
        except Exception as e:
            print(f"DEBUG: Error generando encabezados semanales: {e}"); traceback.print_exc(); self.clear_dashboard(); return

        # --- 4. Configurar la tabla ---
        try:
            num_cols = len(self.week_headers) + 1; num_rows = len(arrivals)
            print(f"DEBUG: Configurando tabla: {num_rows} filas, {num_cols} columnas.") # DEBUG
            if num_rows == 0: print("DEBUG: No hay filas para mostrar en la tabla (arrivals está vacío)."); return
            self.dashboard_table.setRowCount(num_rows); self.dashboard_table.setColumnCount(num_cols)
            col_headers = ["Llegada"] + [f"Sem {d.strftime('%Y-%V')}\n({format_date(d)})" for d in self.week_headers]
            self.dashboard_table.setHorizontalHeaderLabels(col_headers); self.dashboard_table.verticalHeader().setVisible(False)
            print("DEBUG: Tabla configurada.") # DEBUG
        except Exception as e:
            print(f"DEBUG: Error configurando la tabla: {e}"); traceback.print_exc(); self.clear_dashboard(); return

        # --- 5. Preparar seguimiento FIFO para pintar celdas ---
        try:
            timeline_stock_tracking = arrivals[['Arrive Date', 'QTY', 'Expirate Date', 'STATUS']].copy()
            timeline_stock_tracking['Remaining QTY'] = timeline_stock_tracking['QTY'].astype(float)
            timeline_stock_tracking['Initial QTY'] = timeline_stock_tracking['QTY'].astype(float)
            timeline_stock_tracking['Index'] = timeline_stock_tracking.index
            print("DEBUG: Seguimiento FIFO preparado.") # DEBUG
        except Exception as e:
            print(f"DEBUG: Error preparando seguimiento FIFO: {e}"); traceback.print_exc(); return

        # --- 6. Primera columna "Llegada" ---
        try:
            print("DEBUG: Poblando columna 'Llegada'...") # DEBUG
            for i, lot in timeline_stock_tracking.iterrows():
                arrive_date_str = format_date(lot['Arrive Date']); initial_qty_str = f"{lot['Initial QTY']:.0f}" if pd.notna(lot['Initial QTY']) else "N/A"
                status_str = str(lot['STATUS']) if pd.notna(lot['STATUS']) else "N/A"; cell0_text = f"{arrive_date_str}\nQTY: {initial_qty_str}\n({status_str})"
                cell0 = QtWidgets.QTableWidgetItem(cell0_text); cell0.setData(QtCore.Qt.UserRole, i); self.dashboard_table.setItem(i, 0, cell0)
            print("DEBUG: Columna 'Llegada' poblada.") # DEBUG
        except Exception as e:
            print(f"DEBUG: Error poblando columna 'Llegada': {e}"); traceback.print_exc(); return

        # --- 7. Cargar y procesar el log de stock ---
        stock_log_df = pd.DataFrame(); item_log_df = pd.DataFrame(); logged_weeks = {}; arrival_events = []
        print("DEBUG: Procesando log de stock...") # DEBUG
        if not os.path.exists(STOCK_LOG_PATH): print(f"DEBUG: Advertencia: Archivo de log no encontrado en {STOCK_LOG_PATH}")
        else:
            try:
                stock_log_df = pd.read_csv(STOCK_LOG_PATH, encoding="utf-8-sig")
                stock_log_df['EventDate'] = pd.to_datetime(stock_log_df['EventDate'], errors='coerce'); stock_log_df['LogDate'] = pd.to_datetime(stock_log_df['LogDate'], errors='coerce')
                stock_log_df['Arrive Date Parsed'] = stock_log_df['Arrive Date'].apply(lambda x: parse_date(str(x)) if pd.notna(x) else None)
                for col in ['StockBefore', 'StockAfter']: stock_log_df[col] = pd.to_numeric(stock_log_df[col], errors='coerce')
                stock_log_df['Item'] = stock_log_df['Item'].astype(str).fillna("").str.strip(); stock_log_df['EventType'] = stock_log_df['EventType'].astype(str).fillna("").str.strip().str.upper()
                stock_log_df.dropna(subset=['Item', 'EventDate', 'StockBefore', 'StockAfter'], inplace=True)
                item_log_df = stock_log_df[stock_log_df["Item"] == selected_item].copy(); item_log_df.sort_values(by="EventDate", inplace=True)
                log_week_df = item_log_df[item_log_df["EventType"] == "WEEK"].copy()
                log_arrival_df = item_log_df[(item_log_df["EventType"].isin(["ARRIVAL", "PO"])) | (item_log_df['Arrive Date Parsed'].notna() & (item_log_df["EventType"] == "WEEK"))].copy()
                for _, row in log_week_df.iterrows():
                    wk_start = get_week_start(row["EventDate"])
                    if wk_start:
                        if wk_start not in logged_weeks: logged_weeks[wk_start] = []
                        stock_before = float(row["StockBefore"]) if pd.notna(row["StockBefore"]) else 0.0; stock_after = float(row["StockAfter"]) if pd.notna(row["StockAfter"]) else 0.0
                        logged_weeks[wk_start].append((row["EventDate"], stock_before, stock_after))
                for wk_start in logged_weeks: logged_weeks[wk_start].sort(key=lambda x: x[0])
                for _, row in log_arrival_df.iterrows():
                    adp = row['Arrive Date Parsed']
                    if pd.notna(adp) and pd.notna(row["StockAfter"]) and pd.notna(row["StockBefore"]) and row["StockAfter"] > row["StockBefore"]:
                        arrival_events.append({"EventDate": row["EventDate"], "ArriveDate": adp, "QtyAdded": float(row["StockAfter"]) - float(row["StockBefore"])})
                arrival_events.sort(key=lambda x: x['EventDate'])
                print(f"DEBUG: Log procesado. {len(logged_weeks)} semanas con log, {len(arrival_events)} eventos de llegada.") # DEBUG
            except Exception as e:
                print(f"DEBUG: Error leyendo/procesando el log de stock: {e}"); traceback.print_exc()

        # --- 8. Determinar semana actual ---
        try:
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0); self.sim_week_start_dt = get_week_start(today)
            if self.sim_week_start_dt is None: print("DEBUG: Error: No se pudo determinar la semana actual."); self.sim_week_start_dt = self.week_headers[0] if self.week_headers else None;
            if self.sim_week_start_dt is None: return
            print(f"DEBUG: Semana de simulación actual: {format_date(self.sim_week_start_dt)}") # DEBUG
        except Exception as e:
            print(f"DEBUG: Error determinando semana actual: {e}"); traceback.print_exc(); return

        # --- 9. Simulación del "Stock general" para Header ---
        global_current_list = []; global_final_list = []; weekly_demands_header = []; processed_arrival_events_header = set()
        header_start_stock = initial_stock_physical_fallback; last_final_stock_header = header_start_stock
        print("DEBUG: Calculando stock general para header...") # DEBUG
        try:
            for j, week_start in enumerate(self.week_headers):
                week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59); week_key = week_start; current_stock_header = 0.0
                if week_key < self.sim_week_start_dt:
                    if week_key in logged_weeks: current_stock_header = logged_weeks[week_key][0][1]
                    else: current_stock_header = np.nan
                elif week_key == self.sim_week_start_dt:
                    current_stock_header = header_start_stock
                    if week_key in logged_weeks: current_stock_header = logged_weeks[week_key][0][1]
                else: current_stock_header = last_final_stock_header
                global_current_list.append(f"{current_stock_header:.0f}" if pd.notna(current_stock_header) else "-")
                stock_added_from_log_header = 0.0
                for event in arrival_events:
                    event_tuple = (event["EventDate"], event["ArriveDate"])
                    if isinstance(event["ArriveDate"], datetime) and isinstance(event["EventDate"], datetime) and \
                    week_start <= event["EventDate"] <= week_end and event_tuple not in processed_arrival_events_header:
                        stock_added_from_log_header += event["QtyAdded"]; processed_arrival_events_header.add(event_tuple)
                weekly_demand_h = 0.0
                if self.demand_cols:
                    for demand_date_tuple in self.demand_cols:
                        if isinstance(demand_date_tuple, (list, tuple)) and len(demand_date_tuple) >= 2:
                            demand_date, col_name = demand_date_tuple[0], demand_date_tuple[1]
                            if isinstance(demand_date, datetime) and week_start <= demand_date <= week_end:
                                try: weekly_demand_h += float(self.current_inventory_data.get(col_name, 0))
                                except (ValueError, TypeError): pass
                weekly_demands_header.append(weekly_demand_h)
                stock_added_from_pending_header = 0.0
                for _, po_row in arrivals.iterrows():
                    if isinstance(po_row['Arrive Date'], datetime) and po_row['STATUS'] == 'PEDIDO' and \
                    week_start <= po_row['Arrive Date'] <= week_end:
                        try: stock_added_from_pending_header += float(po_row['QTY'])
                        except (ValueError, TypeError): pass
                final_stock_header = 0.0
                if week_key < self.sim_week_start_dt and week_key in logged_weeks: final_stock_header = logged_weeks[week_key][-1][2]
                else:
                    current_stock_for_calc_h = pd.to_numeric(current_stock_header, errors='coerce');
                    if pd.isna(current_stock_for_calc_h): current_stock_for_calc_h = 0.0
                    final_stock_header = current_stock_for_calc_h + stock_added_from_log_header + stock_added_from_pending_header - weekly_demand_h
                global_final_list.append(f"{final_stock_header:.0f}" if pd.notna(final_stock_header) else "-")
                last_final_stock_header = final_stock_header if pd.notna(final_stock_header) else last_final_stock_header
            print("DEBUG: Stock general para header calculado.") # DEBUG
        except Exception as e:
            print(f"DEBUG: Error calculando stock general para header: {e}"); traceback.print_exc(); return

        # ========================================================================
        # --- 10. Simulación virtual de consumo FIFO y Pintado de Celdas ---
        # ========================================================================
        sim_fifo_start_stock = 0.0; first_week_header = self.week_headers[0] if self.week_headers else None
        if first_week_header and not item_log_df.empty:
            logs_before_first_week = item_log_df[item_log_df['EventDate'] < first_week_header].sort_values('EventDate', ascending=False)
            if not logs_before_first_week.empty:
                sim_fifo_start_stock = logs_before_first_week.iloc[0]['StockAfter']
                print(f"DEBUG: Usando StockAfter del log {format_date(logs_before_first_week.iloc[0]['EventDate'])} como inicio FIFO: {sim_fifo_start_stock}")
            else:
                logs_at_first_week_start = item_log_df[item_log_df['EventDate'] == first_week_header].sort_values('EventDate')
                if not logs_at_first_week_start.empty:
                    sim_fifo_start_stock = logs_at_first_week_start.iloc[0]['StockBefore']
                    print(f"DEBUG: Usando StockBefore del log {format_date(logs_at_first_week_start.iloc[0]['EventDate'])} como inicio FIFO: {sim_fifo_start_stock}")
                else:
                    sim_fifo_start_stock = initial_stock_physical_fallback
                    print(f"DEBUG: No hay logs relevantes antes/al inicio de la primera semana. Usando Qty OH ({initial_stock_physical_fallback}) como inicio FIFO.")
        elif not item_log_df.empty:
            sim_fifo_start_stock = initial_stock_physical_fallback; print(f"DEBUG: No hay week_headers? Usando Qty OH ({initial_stock_physical_fallback}) como inicio FIFO.")
        else: sim_fifo_start_stock = initial_stock_physical_fallback; print(f"DEBUG: No hay logs para el item. Usando Qty OH ({initial_stock_physical_fallback}) como inicio FIFO.")
        try: timeline_initial_stock_qty = float(sim_fifo_start_stock)
        except (ValueError, TypeError): print(f"Advertencia: sim_fifo_start_stock ('{sim_fifo_start_stock}') no es numérico. Usando 0."); timeline_initial_stock_qty = 0.0
        print(f"DEBUG: timeline_initial_stock_qty (Stock no asignado a PO) inicializado a: {timeline_initial_stock_qty}") # DEBUG

        timeline_stock_prev_week = timeline_stock_tracking.copy(); weekly_demands_fifo = weekly_demands_header
        print("DEBUG: Iniciando simulación FIFO y pintado de celdas...") # DEBUG

        try:
            for j, week_start in enumerate(self.week_headers):
                week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59); col_index = j + 1
                weekly_demand = weekly_demands_fifo[j] if j < len(weekly_demands_fifo) else 0.0; remaining_demand_for_week = weekly_demand
                consumed_this_week_details = {idx: 0.0 for idx in timeline_stock_tracking.index}

                # --- FASE A: Consumo Stock Inicial (No asignado a PO) ---
                if remaining_demand_for_week > 0 and timeline_initial_stock_qty > 0:
                    consume_from_initial = min(timeline_initial_stock_qty, remaining_demand_for_week)
                    timeline_initial_stock_qty -= consume_from_initial; remaining_demand_for_week -= consume_from_initial
                    print(f"DEBUG: Sem {week_start.strftime('%Y-%V')}: Consumido {consume_from_initial:.1f} de stock inicial. Restante inicial: {timeline_initial_stock_qty:.1f}. Demanda restante: {remaining_demand_for_week:.1f}")

                # --- FASE B: Consumo Lotes Viejos (llegaron ANTES) ---
                if remaining_demand_for_week > 0:
                    for idx in timeline_stock_tracking.index:
                        if remaining_demand_for_week <= 0: break
                        lot = timeline_stock_tracking.loc[idx];
                        if not isinstance(lot['Arrive Date'], datetime): continue
                        is_old_stock = lot['Arrive Date'] < week_start and lot['Remaining QTY'] > 0
                        if is_old_stock:
                            consume_from_this_lot = min(lot['Remaining QTY'], remaining_demand_for_week)
                            if consume_from_this_lot > 0:
                                timeline_stock_tracking.loc[idx, 'Remaining QTY'] -= consume_from_this_lot; consumed_this_week_details[idx] += consume_from_this_lot
                                remaining_demand_for_week -= consume_from_this_lot
                                print(f"DEBUG: Sem {week_start.strftime('%Y-%V')}: Consumido {consume_from_this_lot:.1f} de lote viejo {format_date(lot['Arrive Date'])}. Restante lote: {timeline_stock_tracking.loc[idx, 'Remaining QTY']:.1f}. Demanda restante: {remaining_demand_for_week:.1f}")

                # --- FASE C: Consumo Lotes Nuevos (llegaron DURANTE) ---
                if remaining_demand_for_week > 0:
                    for idx in timeline_stock_tracking.index:
                        if remaining_demand_for_week <= 0: break
                        lot = timeline_stock_tracking.loc[idx];
                        if not isinstance(lot['Arrive Date'], datetime): continue
                        is_new_stock = week_start <= lot['Arrive Date'] <= week_end and lot['Remaining QTY'] > 0
                        if is_new_stock:
                            consume_from_this_lot = min(lot['Remaining QTY'], remaining_demand_for_week)
                            if consume_from_this_lot > 0:
                                timeline_stock_tracking.loc[idx, 'Remaining QTY'] -= consume_from_this_lot; consumed_this_week_details[idx] += consume_from_this_lot
                                remaining_demand_for_week -= consume_from_this_lot
                                print(f"DEBUG: Sem {week_start.strftime('%Y-%V')}: Consumido {consume_from_this_lot:.1f} de lote nuevo {format_date(lot['Arrive Date'])}. Restante lote: {timeline_stock_tracking.loc[idx, 'Remaining QTY']:.1f}. Demanda restante: {remaining_demand_for_week:.1f}")

                # --- FASE D: Pintar Celdas ---
                for i, lot in timeline_stock_tracking.iterrows():
                    if i >= self.dashboard_table.rowCount() or col_index >= self.dashboard_table.columnCount(): continue
                    cell_item = QtWidgets.QTableWidgetItem(); item_col0 = self.dashboard_table.item(i, 0);
                    if item_col0 is None: continue
                    cell_item.setData(QtCore.Qt.UserRole, i)
                    available_qty = lot['Remaining QTY']; initial_qty = lot['Initial QTY']; prev_available_qty = timeline_stock_prev_week.loc[i, 'Remaining QTY']
                    arr_date = lot['Arrive Date']; exp_date = lot['Expirate Date']; lot_status_original = lot['STATUS']
                    lot_week_start = get_week_start(arr_date) if isinstance(arr_date, datetime) else None
                    consumed_this_week = consumed_this_week_details.get(i, 0.0) > 0
                    cell_text = ""

                    # --- Lógica de Texto (Ajuste para "Agotado") ---
                    if not isinstance(arr_date, datetime): cell_text = "" # Celda vacía si la fecha no es válida
                    elif week_start < lot_week_start: cell_text = f"Llega {format_date(arr_date)}"
                    elif week_start == lot_week_start: # Semana de llegada
                        if consumed_this_week: cell_text = f"Consumo ({available_qty:.0f})"
                        elif available_qty == initial_qty :
                            arrival_confirmed = any(isinstance(evt.get("ArriveDate"), datetime) and evt["ArriveDate"] == arr_date and isinstance(evt.get("EventDate"), datetime) and week_start <= evt["EventDate"] <= week_end for evt in arrival_events)
                            if (arrival_confirmed or lot_status_original == 'ENTREGADO'): cell_text = f"Stock (+{initial_qty:.0f})"
                            else: cell_text = f"Pendiente ({initial_qty:.0f})"
                        else: cell_text = f"Consumo ({available_qty:.0f})" # O Stock si no se consumió esta semana
                    else: # Semanas posteriores a la llegada
                        is_expired = isinstance(exp_date, datetime) and week_start >= exp_date
                        if is_expired and prev_available_qty > 0: cell_text = f"Expirado ({prev_available_qty:.0f})"
                        # *** INICIO AJUSTE AGOTADO ***
                        elif available_qty <= 0:
                            # Mostrar "Agotado" SOLO si se agotó ESTA semana
                            if prev_available_qty > 0:
                                cell_text = "Agotado"
                            else:
                                # Si ya estaba agotado antes, dejar la celda vacía
                                cell_text = ""
                        # *** FIN AJUSTE AGOTADO ***
                        elif consumed_this_week: cell_text = f"Consumo ({available_qty:.0f})"
                        elif available_qty > 0: cell_text = f"Stock ({available_qty:.0f})"
                    # --- Fin Lógica de Texto ---

                    cell_item.setText(cell_text); cell_item.setTextAlignment(QtCore.Qt.AlignCenter)
                    self.dashboard_table.setItem(i, col_index, cell_item)

                timeline_stock_prev_week = timeline_stock_tracking.copy()
            print("DEBUG: Simulación FIFO y pintado de celdas completado.") # DEBUG

        except Exception as e:
            print(f"DEBUG: Error durante simulación FIFO o pintado: {e}"); traceback.print_exc()

        # --- 11. Actualizar el encabezado global personalizado ---
        try:
            print("DEBUG: Actualizando header global...") # DEBUG
            header = self.dashboard_table.horizontalHeader()
            if hasattr(header, 'setGlobalValues'):
                header_len = len(self.week_headers); header.setGlobalValues(global_current_list[:header_len], global_final_list[:header_len])
                print("DEBUG: Header global actualizado.") # DEBUG
            else: print("DEBUG: Advertencia: El header horizontal no tiene el método 'setGlobalValues'.")
        except Exception as e:
            print(f"DEBUG: Error actualizando header global: {e}"); traceback.print_exc()

        # --- 12. Ajustes finales y agrupación ---
        try:
            print("DEBUG: Ajustando tamaño de columnas/filas y agrupando...") # DEBUG
            self.dashboard_table.resizeColumnsToContents(); self.dashboard_table.resizeRowsToContents()
            if hasattr(self, 'updateColumnGrouping'): self.updateColumnGrouping(); print("DEBUG: Agrupamiento actualizado.") # DEBUG
            else: print("DEBUG: Advertencia: El método 'updateColumnGrouping' no está definido.")
        except Exception as e:
            print(f"DEBUG: Error en ajustes finales o agrupación: {e}"); traceback.print_exc()

        print("DEBUG: Fin populate_dashboard.") # DEBUG

    # --- FIN populate_dashboard ---

    # ================================================================
    # MÉTODO updateColumnGrouping CORREGIDO
    # ================================================================
    def updateColumnGrouping(self):
        """
        Calcula qué columnas del dashboard corresponden a semanas pasadas
        en comparación con la semana de simulación actual (self.sim_week_start_dt).
        """
        # Asegurarse que los atributos necesarios existen
        if not hasattr(self, 'week_headers') or not self.week_headers or not hasattr(self, 'sim_week_start_dt') or self.sim_week_start_dt is None:
             # print("Advertencia: No se puede actualizar agrupamiento, faltan week_headers o sim_week_start_dt.")
             return

        group_columns_indices = [] # Índices de columnas en la *tabla* (a partir de 1)

        # Iterar sobre las semanas del header generadas en populate_dashboard
        for j, week_start_header in enumerate(self.week_headers):
             col_index_in_table = j + 1 # Columna 1 en adelante
             # Agrupar si el inicio de la semana del header es ANTERIOR al inicio de la semana de simulación actual
             if week_start_header < self.sim_week_start_dt:
                  group_columns_indices.append(col_index_in_table)

        self.grouped_columns = group_columns_indices
        # print(f"DEBUG: Columns to group (indices < {format_date(self.sim_week_start_dt)}): {self.grouped_columns}") # Debug opcional

        # Gestionar botón de grupo
        if self.grouped_columns:
            # Si hay columnas para agrupar, asegurar que el botón existe y es visible
            if not hasattr(self, "groupButton") or self.groupButton is None:
                 self.setupGroupButton() # Crear si no existe
            elif not self.groupButton.isVisible():
                 self.groupButton.show() # Mostrar si estaba oculto

            # Ocultar/Mostrar columnas según el estado actual del botón
            if hasattr(self, "groupButton") and self.groupButton: # Re-verificar por si setup falló
                 is_currently_grouped = (self.groupButton.text() == "+")
                 for col in self.grouped_columns:
                      if col < self.dashboard_table.columnCount():
                           self.dashboard_table.setColumnHidden(col, is_currently_grouped)
                 # Reposicionar el botón
                 QtCore.QTimer.singleShot(0, self.repositionGroupButton)

        elif hasattr(self, "groupButton") and self.groupButton:
             # Si no hay columnas para agrupar, ocultar el botón
             self.groupButton.hide()


    def append_log_event(self, log_line):
        """
        Registra una línea de evento en el log de stock (STOCK_LOG_TOOLTRACK+.csv).
        """
        try:
            with open(STOCK_LOG_PATH, "a", encoding="utf-8-sig") as log_file:
                log_file.write(log_line + "\n")
        except Exception as e:
            print(f"Error al escribir en el log: {e}")

    def confirm_arrivals(self):
        """
        Permite confirmar o modificar la llegada de POs pendientes (con Arrive Date <= hoy y STATUS "PEDIDO").
        Al confirmar se actualiza el STATUS a 'ENTREGADO', se suma el QTY al inventario y se registra en el log (EventType "PO").
        """
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        pending = self.po_df[(self.po_df['Arrive Date'] <= today) & (self.po_df['STATUS'] == 'PEDIDO')]
        if pending.empty:
            QtWidgets.QMessageBox.information(self, "Sin Pendientes", "No hay llegadas pendientes para confirmar.")
            return

        updates_made = False
        indices_to_update = []
        log_records = []

        try:
            inv_df = pd.read_csv(CONSUMABLE_INVENTORY_PATH, encoding="utf-8-sig")
            inv_df['Item'] = inv_df['Item'].astype(str)
            inv_df['Qty OH'] = pd.to_numeric(inv_df['Qty OH'], errors='coerce').fillna(0)
        except Exception as e_inv:
            QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo cargar el inventario:\n{e_inv}")
            traceback.print_exc()
            return

        for index, row in pending.iterrows():
            item = row['Item']
            arrival_str = format_date(row['Arrive Date'])
            qty = row['QTY']
            reply = QtWidgets.QMessageBox.question(
                self,
                "Confirmar Llegada",
                f"PO para Item: {item}\nFecha Llegada: {arrival_str}\nCantidad: {qty}\n\n¿Confirma la llegada? (Sí = Confirmar, No = Modificar Fecha)",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No | QtWidgets.QMessageBox.Cancel,
                QtWidgets.QMessageBox.Cancel
            )
            
            if reply == QtWidgets.QMessageBox.Yes:
                indices_to_update.append(index)
                updates_made = True
                mask = inv_df['Item'] == item
                if mask.any():
                    current_qty = inv_df.loc[mask, 'Qty OH'].iloc[0]
                    new_qty = current_qty + qty
                    inv_df.loc[mask, 'Qty OH'] = new_qty
                    log_record = {
                        "Item": item,
                        "EventType": "PO",
                        "EventDate": row['Arrive Date'],
                        "StockBefore": current_qty,
                        "StockAfter": new_qty,
                        "LogDate": datetime.now()
                    }
                    log_records.append(log_record)
                else:
                    QtWidgets.QMessageBox.warning(self, "Sin Inventario",
                        f"Item {item} no se encontró en el inventario.")
            elif reply == QtWidgets.QMessageBox.No:
                dialog = ModifyDateDialog(current_date=row['Arrive Date'], parent=self)
                if dialog.exec_() == QtWidgets.QDialog.Accepted:
                    new_arrival = dialog.getSelectedDate()
                    if new_arrival:
                        exp_days = row['Expirate Days']
                        new_exp_date = new_arrival + timedelta(days=int(exp_days)) if exp_days > 0 else None
                        self.po_df.loc[index, 'Arrive Date'] = new_arrival
                        self.po_df.loc[index, 'Expirate Date'] = new_exp_date
                        updates_made = True
                    else:
                        QtWidgets.QMessageBox.warning(self, "Fecha inválida", "No se seleccionó una fecha válida.")
            elif reply == QtWidgets.QMessageBox.Cancel:
                QtWidgets.QMessageBox.information(self, "Cancelado", "Proceso de confirmación cancelado.")
                return

        if indices_to_update:
            self.po_df.loc[indices_to_update, 'STATUS'] = 'ENTREGADO'
        
        if updates_made:
            self.save_po_data()
            try:
                inv_df.to_csv(CONSUMABLE_INVENTORY_PATH, index=False, encoding="utf-8-sig")
                print("Inventario actualizado.")
            except Exception as e_inv:
                QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo actualizar el inventario:\n{e_inv}")
                traceback.print_exc()
            if log_records:
                try:
                    try:
                        stock_log_df = pd.read_csv(STOCK_LOG_PATH, encoding="utf-8-sig", parse_dates=["EventDate", "LogDate"])
                    except Exception:
                        stock_log_df = pd.DataFrame(columns=["Item", "EventType", "EventDate", "StockBefore", "StockAfter", "LogDate"])
                    new_log_df = pd.DataFrame(log_records)
                    stock_log_df = pd.concat([stock_log_df, new_log_df], ignore_index=True)
                    stock_log_df.to_csv(STOCK_LOG_PATH, index=False, encoding="utf-8-sig")
                    print("Log de stock actualizado.")
                except Exception as log_ex:
                    print(f"Error al guardar el log de stock: {log_ex}")
            self.load_data()

    def on_cell_double_clicked(self, row, column):
        """Permite modificar el 'Arrive Date' al hacer doble clic en la columna 'Llegada'."""
        if column != 0:
            return
        cell = self.dashboard_table.item(row, 0)
        if not cell:
            return
        original_idx = cell.data(QtCore.Qt.UserRole)
        try:
            po_row = self.po_df.loc[original_idx]
            current_arrival = po_row['Arrive Date']
            exp_days = po_row['Expirate Days']
            dialog = ModifyDateDialog(current_date=current_arrival, parent=self)
            if dialog.exec_() == QtWidgets.QDialog.Accepted:
                new_arrival = dialog.getSelectedDate()
                if new_arrival is None:
                    QtWidgets.QMessageBox.warning(self, "Fecha inválida", "No se seleccionó una fecha válida.")
                    return
                new_exp_date = new_arrival + timedelta(days=int(exp_days)) if exp_days > 0 else None
                self.po_df.loc[original_idx, 'Arrive Date'] = new_arrival
                self.po_df.loc[original_idx, 'Expirate Date'] = new_exp_date
                self.save_po_data()
                self.on_item_selected(self.item_combo.currentIndex())
        except KeyError:
            QtWidgets.QMessageBox.critical(self, "Error", f"No se encontró el índice {original_idx} en los POs.")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Ocurrió un error:\n{e}")
            traceback.print_exc()

    def save_po_data(self):
        """Guarda el DataFrame de POs en el archivo CSV, formateando las fechas."""
        if self.po_df.empty:
            print("No hay datos de PO para guardar.")
            return
        try:
            df_to_save = self.po_df.copy()
            df_to_save['Arrive Date'] = df_to_save['Arrive Date'].apply(format_date)
            df_to_save['Expirate Date'] = df_to_save['Expirate Date'].apply(format_date)
            df_to_save.to_csv(DASHBOARD_PATH, index=False, encoding="utf-8-sig")
            print(f"POs guardados en: {DASHBOARD_PATH}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error al Guardar", f"No se pudo guardar el archivo de POs:\n{e}")
            traceback.print_exc()

    def save_inventory_data(self):
        try:
            self.inventory_df.to_csv(CONSUMABLE_INVENTORY_PATH, index=False, encoding="utf-8-sig")
            print("Inventario actualizado en el archivo.")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error al guardar inventario", f"No se pudo guardar el inventario:\n{e}")
    def manage_inventory(self):
        """
        Método que se ejecuta al presionar el botón "Administrar inventario físico".
        Permite actualizar el inventario físico para el ítem seleccionado y
        registra el cambio en el log.
        """
        # Obtener el ítem seleccionado (se asume que el combobox ya está poblado)
        item = self.item_combo.currentText().strip()
        if not item:
            QtWidgets.QMessageBox.warning(self, "Error", "No se seleccionó ningún ítem.")
            return

        # Obtener la cantidad actual del inventario para el ítem (ejemplo: se usa current_inventory_data)
        current_qty = self.current_inventory_data.get('Qty OH', 0)
        
        # Mostrar un diálogo para actualizar la cantidad; se pre-carga el valor actual.
        new_qty, ok = QtWidgets.QInputDialog.getDouble(
            self,
            "Actualizar Inventario Físico",
            f"Inventario actual para {item}: {current_qty}\nIngrese la nueva cantidad:",
            current_qty,
            0
        )
        
        if ok:
            # Actualizar el DataFrame de inventario para el ítem
            self.inventory_df.loc[self.inventory_df['Item'] == item, 'Qty OH'] = new_qty
            print(f"DEBUG: Se actualizó 'Qty OH' para {item} a {new_qty}.")
            
            # Registrar el cambio en el log
            self.log_inventory_change(item, new_qty)
        else:
            print("DEBUG: Actualización cancelada por el usuario.")

    def log_inventory_change(self, item, new_physical):
        """
        Registra el cambio en el log de stock:
        - StockBefore registra el nuevo inventario físico ingresado.
        - Se calcula la demanda estimada según la fecha actual (usando estimate_consumption)
            y se resta al nuevo inventario para definir StockAfter.
        """
        event_type = "Week"  # Puedes ajustar según tu lógica
        event_date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        stock_before = new_physical
        # Calcular la demanda estimada para la semana actual
        estimated_demand = self.estimate_consumption(item)
        # StockAfter es el nuevo stock menos la demanda estimada (si deseas otro cálculo, adáptalo)
        stock_after = new_physical - estimated_demand
        log_date_str = event_date_str
        arrive_date_str = ""  # Puedes asignarlo si requieres

        # Construir la línea de log en formato CSV
        log_line = f"{item},{event_type},{event_date_str},{stock_before},{stock_after},{log_date_str},{arrive_date_str}"
        
        # Usar la función helper para agregar la línea al log
        self.append_log_event(log_line)
        print(f"DEBUG: Se registró en el log el cambio: {log_line}")

    def estimate_consumption(self, item):
        """
        Calcula la demanda estimada para el ítem basándose en la semana en curso.
        Se utiliza la función get_week_start para obtener el lunes de la semana actual
        y se define el domingo sumándole 6 días. Luego se recorre self.demand_cols (que
        debe estar ordenada cronológicamente) y se devuelve el valor de la primera columna
        cuya fecha esté dentro de ese rango. Si no se encuentra dicha columna, se asume demanda 0.
        """
        # Obtener la fecha actual y calcular el límite de la semana
        today = datetime.now()
        week_start = get_week_start(today)  # Función que devuelve el lunes de la semana
        week_end = week_start + timedelta(days=6)

        upcoming_demand = 0.0

        # Recorrer las columnas de demanda (cada elemento es una tupla: (fecha_obj, nombre_columna))
        for dt_obj, col in self.demand_cols:
            # Si la fecha de la columna se encuentra dentro de la semana actual
            if week_start <= dt_obj <= week_end:
                row = self.inventory_df[self.inventory_df['Item'] == item]
                if not row.empty:
                    try:
                        upcoming_demand = float(row.iloc[0][col])
                    except (ValueError, KeyError):
                        upcoming_demand = 0.0
                break  # Utilizamos sólo la primera columna encontrada en esta semana
        return upcoming_demand

    def setupGroupButton(self):
        """Crea y posiciona el QToolButton para agrupar/desagrupar las columnas pasadas."""
        self.groupButton = QtWidgets.QToolButton(self.dashboard_table)
        self.groupButton.setText("+")
        self.groupButton.setStyleSheet("background-color: lightgray;")
        self.groupButton.clicked.connect(self.toggleGroupedColumns)
        for col in self.grouped_columns:
            self.dashboard_table.hideColumn(col)
        QtCore.QTimer.singleShot(0, self.repositionGroupButton)
        self.dashboard_table.horizontalHeader().sectionResized.connect(self.repositionGroupButton)

    def repositionGroupButton(self):
        header = self.dashboard_table.horizontalHeader()
        if not self.grouped_columns or self.groupButton is None:
            return
        first_group_col = self.grouped_columns[0]
        margin = 2
        btn_width = 20
        btn_height = 20
        # Calculamos la posición X: posición de la sección +
        # el ancho de la sección menos el ancho del botón y un margen.
        x = header.sectionPosition(first_group_col) + header.sectionSize(first_group_col) - btn_width - margin
        # Para la posición Y, usamos un margen superior.
        y = margin
        self.groupButton.setGeometry(x, y, btn_width, btn_height)

    def toggleGroupedColumns(self):
        """Alterna la visibilidad de las columnas agrupadas y actualiza el texto del botón."""
        if self.groupButton.text() == "+":
            for col in self.grouped_columns:
                self.dashboard_table.showColumn(col)
            self.groupButton.setText("–")
        else:
            for col in self.grouped_columns:
                self.dashboard_table.hideColumn(col)
            self.groupButton.setText("+")
        self.dashboard_table.horizontalHeader().update()

# ================================================================
# Diálogo para Agregar Nuevo Item (AddItemDialog)
# MODIFICADO: Guarda ruta local en el DataFrame y copia imagen localmente
# ================================================================
class AddItemDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Agregar Nuevo Item")
        self.image_path = None  # Ruta de la imagen seleccionada por el usuario

        layout = QtWidgets.QFormLayout(self)

        self.txt_itemcode = QtWidgets.QLineEdit()
        layout.addRow("ItemCode:", self.txt_itemcode)

        self.txt_description = QtWidgets.QLineEdit()
        layout.addRow("Description:", self.txt_description)

        img_layout = QtWidgets.QHBoxLayout()
        self.lbl_image = QtWidgets.QLabel("No image selected")
        self.btn_cargar = QtWidgets.QPushButton("Cargar imagen")
        self.btn_cargar.clicked.connect(self.cargar_imagen)
        img_layout.addWidget(self.lbl_image)
        img_layout.addWidget(self.btn_cargar)
        layout.addRow("Foto:", img_layout)

        btn_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        btn_box.accepted.connect(self.validar)
        btn_box.rejected.connect(self.reject)
        layout.addRow(btn_box)

    def cargar_imagen(self):
        filepath, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Seleccionar imagen", "", "Imagenes (*.png *.jpg *.jpeg *.PNG *.JPG *.JPEG)")
        if filepath:
            self.image_path = filepath
            self.lbl_image.setText(os.path.basename(filepath))

    def validar(self):
        itemcode = self.txt_itemcode.text().strip().upper() # Convertir a mayúsculas
        description = self.txt_description.text().strip()
        if not itemcode or not description:
            QtWidgets.QMessageBox.warning(self, "Advertencia", "Complete ItemCode y Description.")
            return
        # Validar formato de ItemCode si es necesario (ej: usando regex)
        # if not re.match(r"^[A-Z0-9-]+$", itemcode): # Ejemplo de validación
        #     QtWidgets.QMessageBox.warning(self, "Advertencia", "ItemCode inválido. Use solo mayúsculas, números y guiones.")
        #     return

        if not self.image_path:
            QtWidgets.QMessageBox.warning(self, "Advertencia", "Debe seleccionar una imagen.")
            return
        self.accept()

    def getData(self):
        """
        Devuelve los datos del nuevo item, incluyendo la ruta original de la imagen
        seleccionada por el usuario. La copia y generación de rutas finales
        se hará en la lógica que llama a este diálogo (Window2Page.agregar_item).
        """
        return {
            "ItemCode": self.txt_itemcode.text().strip().upper(), # Asegurar mayúsculas
            "Description": self.txt_description.text().strip(),
            "SelectedImagePath": self.image_path # Ruta original seleccionada
        }

# ================================================================
# Diálogo para Ver/Modificar Detalles de un Item (ItemDetailsDialog)
# MODIFICADO: Prioriza carga local, maneja ruta local al guardar/borrar
# ================================================================
class ItemDetailsDialog(QtWidgets.QDialog):
    def __init__(self, item_data, user_alias, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Detalle del Item")
        self.resize(800, 500)
        self.item_data = item_data.copy() # Diccionario con datos de la fila
        self.user_alias = user_alias
        self.new_image_path_source = None # Ruta de la *nueva* imagen seleccionada por el usuario
        self.image_deleted = False # Flag para indicar si se borró la imagen
        self.edit_mode = False

        main_layout = QtWidgets.QVBoxLayout(self)
        top_layout = QtWidgets.QHBoxLayout()

        # Izquierdo: Datos
        form_layout = QtWidgets.QFormLayout()
        self.itemcode_edit = QtWidgets.QLineEdit(str(self.item_data.get("ItemCode", "")))
        self.itemcode_edit.setReadOnly(True) # ItemCode no debería ser editable aquí
        form_layout.addRow("ItemCode:", self.itemcode_edit)

        self.description_edit = QtWidgets.QLineEdit(str(self.item_data.get("Description", "")))
        self.description_edit.setReadOnly(True)
        form_layout.addRow("Description:", self.description_edit)
        top_layout.addLayout(form_layout)

        # Derecho: Imagen grande
        self.image_label = QtWidgets.QLabel()
        self.image_label.setFixedSize(300, 300)
        self.image_label.setAlignment(QtCore.Qt.AlignCenter)
        self.image_label.setStyleSheet("border: 1px solid #ccc;") # Estilo
        # Cargar imagen inicial (priorizando local)
        self.load_image()
        top_layout.addWidget(self.image_label)

        main_layout.addLayout(top_layout)

        # Panel para botones de imagen (solo visibles en modo edición)
        image_buttons_layout = QtWidgets.QHBoxLayout()
        self.btn_cambiar_imagen = QtWidgets.QPushButton("Cambiar imagen")
        self.btn_cambiar_imagen.clicked.connect(self.cambiar_imagen)
        self.btn_cambiar_imagen.setVisible(False)
        image_buttons_layout.addWidget(self.btn_cambiar_imagen)

        self.btn_borrar_imagen = QtWidgets.QPushButton("Borrar imagen")
        self.btn_borrar_imagen.clicked.connect(self.borrar_imagen)
        self.btn_borrar_imagen.setVisible(False)
        image_buttons_layout.addWidget(self.btn_borrar_imagen)
        main_layout.addLayout(image_buttons_layout)

        # Pie del diálogo: Botones
        button_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Close)
        self.btn_modificar = QtWidgets.QPushButton("Modificar")
        self.btn_modificar.clicked.connect(self.modificar)
        # Deshabilitar Modificar si no hay permisos inicialmente
        self.btn_modificar.setEnabled(check_update_permission(Session.user_alias))

        button_box.addButton(self.btn_modificar, QtWidgets.QDialogButtonBox.ActionRole)
        # button_box.accepted.connect(self.accept) # Accept se maneja en guardar_cambios
        button_box.rejected.connect(self.reject)
        main_layout.addWidget(button_box)

    def load_image(self):
        """Carga la imagen priorizando la ruta local."""
        local_path = str(self.item_data.get("Foto Local", ""))
        network_path = str(self.item_data.get("Foto", "")) # Ruta de red como fallback
        image_to_load = ""

        # 1. Intentar ruta local
        if local_path and local_path.lower() != 'nan' and os.path.isfile(local_path):
            image_to_load = local_path
            # print(f"Cargando imagen local: {local_path}")
        # 2. Si falla local, intentar ruta de red
        elif network_path and network_path.lower() != 'nan' and os.path.isfile(network_path):
            image_to_load = network_path
            # print(f"Cargando imagen de red (fallback): {network_path}")
        # 3. Si ambas fallan
        else:
            self.image_label.setText("No Image")
            self.image_label.setPixmap(QtGui.QPixmap()) # Limpiar pixmap previo
            # print(f"No se encontró imagen local ('{local_path}') ni de red ('{network_path}')")
            return

        # Cargar la imagen encontrada
        if image_to_load:
            pixmap = QtGui.QPixmap(image_to_load)
            if pixmap.isNull():
                self.image_label.setText("Error al cargar")
                self.image_label.setPixmap(QtGui.QPixmap())
                print(f"Error: QPixmap nulo para la ruta: {image_to_load}")
            else:
                pixmap = pixmap.scaled(300, 300, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
                self.image_label.setPixmap(pixmap)
                self.image_label.setText("") # Limpiar texto si la imagen carga
        else:
             # Esto no debería ocurrir si la lógica anterior es correcta, pero por si acaso
             self.image_label.setText("No Image")
             self.image_label.setPixmap(QtGui.QPixmap())


    def modificar(self):
        # La comprobación de permisos ya se hizo al crear el botón
        # Habilitar edición de campos (solo Description)
        # self.itemcode_edit.setReadOnly(False) # No permitir editar ItemCode
        self.description_edit.setReadOnly(False)
        self.btn_cambiar_imagen.setVisible(True)
        self.btn_borrar_imagen.setVisible(True)
        self.edit_mode = True
        # Cambiar el texto del botón "Modificar" a "Guardar" y reconectar
        self.btn_modificar.setText("Guardar")
        try:
            self.btn_modificar.clicked.disconnect()
        except TypeError: pass
        self.btn_modificar.clicked.connect(self.guardar_cambios)

    def cambiar_imagen(self):
        filepath, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Seleccionar nueva imagen", "", "Imagenes (*.png *.jpg *.jpeg *.PNG *.JPG *.JPEG)")
        if filepath:
            self.new_image_path_source = filepath
            self.image_deleted = False # Si se cambia, no está borrada
            # Mostrar vista previa de la nueva imagen
            pixmap = QtGui.QPixmap(filepath)
            if not pixmap.isNull():
                pixmap = pixmap.scaled(300, 300, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
                self.image_label.setPixmap(pixmap)
                self.image_label.setText("")
            else:
                self.image_label.setText("Error Previa")
                self.image_label.setPixmap(QtGui.QPixmap())


    def borrar_imagen(self):
        reply = QtWidgets.QMessageBox.question(
            self, "Confirmar Borrado", "¿Estás seguro de borrar la imagen actual?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No
        )
        if reply == QtWidgets.QMessageBox.Yes:
            self.image_deleted = True
            self.new_image_path_source = None # Anular si se había seleccionado una nueva
            self.image_label.setText("Imagen Borrada")
            self.image_label.setPixmap(QtGui.QPixmap()) # Limpiar vista previa
            QtWidgets.QMessageBox.information(self, "Imagen Marcada", "La imagen se eliminará al guardar.")

    def guardar_cambios(self):
        # Validar campos si es necesario
        new_description = self.description_edit.text().strip()
        if not new_description:
             QtWidgets.QMessageBox.warning(self, "Advertencia", "La descripción no puede estar vacía.")
             return

        # Actualizar datos en el diccionario interno
        self.item_data["Description"] = new_description
        itemcode = str(self.item_data.get("ItemCode", "")) # Obtener ItemCode (no editable)

        # --- Lógica de manejo de archivos de imagen ---
        current_local_path = str(self.item_data.get("Foto Local", ""))
        current_network_path = str(self.item_data.get("Foto", ""))

        # 1. Si se marcó para borrar
        if self.image_deleted:
            print(f"Intentando borrar imágenes para {itemcode}...")
            # Borrar archivo local si existe
            if current_local_path and os.path.isfile(current_local_path):
                try:
                    os.remove(current_local_path)
                    print(f"Archivo local borrado: {current_local_path}")
                except Exception as e:
                    print(f"Error al borrar archivo local {current_local_path}: {e}")
                    # No crítico, continuar para borrar de red si es posible
            # Borrar archivo de red si existe
            if current_network_path and os.path.isfile(current_network_path):
                try:
                    os.remove(current_network_path)
                    print(f"Archivo de red borrado: {current_network_path}")
                except Exception as e:
                    print(f"Error al borrar archivo de red {current_network_path}: {e}")
                    QtWidgets.QMessageBox.warning(self, "Error al Borrar", f"No se pudo borrar el archivo de imagen en la red:\n{e}\nEs posible que necesites borrarlo manualmente.")
            # Limpiar rutas en los datos
            self.item_data["Foto"] = ""
            self.item_data["Foto Local"] = ""

        # 2. Si se seleccionó una nueva imagen
        elif self.new_image_path_source:
            print(f"Procesando nueva imagen para {itemcode} desde {self.new_image_path_source}")
            # Determinar extensión y nombre base
            _, ext = os.path.splitext(self.new_image_path_source)
            if not ext: ext = ".png" # Default a png si no hay extensión
            dest_filename_base = itemcode # Usar ItemCode como nombre base
            dest_filename = f"{dest_filename_base}{ext.lower()}" # Nombre final con extensión

            # Rutas de destino
            dest_local_path = os.path.join(LOCAL_IMAGENES_CAT_PATH, dest_filename)
            dest_network_path = os.path.join(IMAGENES_CAT_PATH, dest_filename)

            # Borrar imágenes anteriores (local y red) si existían y tenían otro nombre/extensión
            if current_local_path and os.path.isfile(current_local_path) and current_local_path != dest_local_path:
                try: os.remove(current_local_path); print(f"Borrada imagen local anterior: {current_local_path}")
                except Exception as e: print(f"Error borrando local anterior: {e}")
            if current_network_path and os.path.isfile(current_network_path) and current_network_path != dest_network_path:
                try: os.remove(current_network_path); print(f"Borrada imagen red anterior: {current_network_path}")
                except Exception as e: print(f"Error borrando red anterior: {e}")

            # Copiar la nueva imagen a local y red
            copy_success = True
            try:
                os.makedirs(LOCAL_IMAGENES_CAT_PATH, exist_ok=True)
                shutil.copy2(self.new_image_path_source, dest_local_path)
                print(f"Nueva imagen copiada a local: {dest_local_path}")
            except Exception as e:
                print(f"Error al copiar nueva imagen a local: {e}")
                QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo copiar la nueva imagen a la carpeta local:\n{e}")
                copy_success = False

            if copy_success: # Solo intentar copiar a red si la copia local fue exitosa
                try:
                    os.makedirs(IMAGENES_CAT_PATH, exist_ok=True)
                    shutil.copy2(self.new_image_path_source, dest_network_path)
                    print(f"Nueva imagen copiada a red: {dest_network_path}")
                    # Actualizar rutas en los datos solo si ambas copias (o al menos la local) fueron exitosas
                    self.item_data["Foto"] = dest_network_path
                    self.item_data["Foto Local"] = dest_local_path
                except Exception as e:
                    print(f"Error al copiar nueva imagen a red: {e}")
                    QtWidgets.QMessageBox.warning(self, "Error de Red", f"No se pudo copiar la nueva imagen a la carpeta de red:\n{e}\nLa imagen se guardó localmente, pero puede no estar visible para otros usuarios hasta que se copie manualmente.")
                    # Aún así, actualizamos las rutas porque la local sí se copió
                    self.item_data["Foto"] = dest_network_path # Guardar la ruta de red esperada
                    self.item_data["Foto Local"] = dest_local_path

        # 3. Si no se borró ni se cambió, no hacer nada con los archivos/rutas de imagen

        # --- Fin lógica de manejo de archivos ---

        self.accept() # Cerrar el diálogo si todo fue bien

    def getData(self):
        """Devuelve los datos actualizados del item."""
        return self.item_data

# ================================================================
# Pestaña Principal (Window2Page) - MODIFICADA para incluir Caducidad PO
# ================================================================
class Window2Page(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        # self.user_alias = Session.user_alias # Obtener alias de la sesión
        self.df = pd.DataFrame() # DataFrame para el catálogo
        self.initUI()
        # La carga de datos del catálogo se hace en setup_catalog_ui -> load_data
        # La carga de datos de herramentales se hace en InventarioHerramentalesPage.__init__
        # La carga de datos de caducidad se hace en CaducidadPOTab.__init__

    def initUI(self):
        main_layout = QtWidgets.QVBoxLayout(self)
        self.inner_tab = QtWidgets.QTabWidget(self) # Tab principal para esta sección

        # Pestaña 1: Catálogo Indirectos
        self.catalog_tab = QtWidgets.QWidget()
        self.setup_catalog_ui(self.catalog_tab) # Configura y carga datos del catálogo
        self.inner_tab.addTab(self.catalog_tab, "Catálogo Indirectos")

        # Pestaña 2: Inventario de Herramentales
        self.herramentales_tab = QtWidgets.QWidget() # Contenedor
        try:
            # Instanciar la página de herramentales aquí
            self.herramentales_page = InventarioHerramentalesPage(parent=self) # Crear la instancia
            # Crear un layout para el contenedor y añadir la página
            herram_layout = QtWidgets.QVBoxLayout(self.herramentales_tab)
            herram_layout.setContentsMargins(0,0,0,0) # Sin márgenes extra
            herram_layout.addWidget(self.herramentales_page)
            self.inner_tab.addTab(self.herramentales_tab, "Inventario de Herramentales")
        except NameError:
            print("Advertencia: La clase 'InventarioHerramentalesPage' no está definida aún.")
            # Podrías añadir un label indicando que no está disponible
            error_label = QtWidgets.QLabel("Módulo de Inventario de Herramentales no disponible.")
            error_layout = QtWidgets.QVBoxLayout(self.herramentales_tab)
            error_layout.addWidget(error_label, alignment=QtCore.Qt.AlignCenter)
            self.inner_tab.addTab(self.herramentales_tab, "Inventario de Herramentales [No Disponible]")
        except Exception as e:
            print(f"Error al instanciar InventarioHerramentalesPage: {e}")
            traceback.print_exc()
            # Añadir pestaña con mensaje de error
            error_label = QtWidgets.QLabel(f"Error al cargar módulo Inventario:\n{e}")
            error_layout = QtWidgets.QVBoxLayout(self.herramentales_tab)
            error_layout.addWidget(error_label, alignment=QtCore.Qt.AlignCenter)
            self.inner_tab.addTab(self.herramentales_tab, "Inventario de Herramentales [Error]")

        # --- NUEVA PESTAÑA: Caducidad por PO ---
        self.caducidad_tab = QtWidgets.QWidget() # Contenedor
        try:
            self.caducidad_page = CaducidadPOTab(parent=self) # Crear instancia
            caducidad_layout = QtWidgets.QVBoxLayout(self.caducidad_tab)
            caducidad_layout.setContentsMargins(0,0,0,0)
            caducidad_layout.addWidget(self.caducidad_page)
            self.inner_tab.addTab(self.caducidad_tab, "Caducidad por PO")
        except NameError:
             print("Advertencia: La clase 'CaducidadPOTab' no está definida.")
             error_label = QtWidgets.QLabel("Módulo de Caducidad por PO no disponible.")
             error_layout = QtWidgets.QVBoxLayout(self.caducidad_tab)
             error_layout.addWidget(error_label, alignment=QtCore.Qt.AlignCenter)
             self.inner_tab.addTab(self.caducidad_tab, "Caducidad por PO [No Disponible]")
        except Exception as e:
            print(f"Error al instanciar CaducidadPOTab: {e}")
            traceback.print_exc()
            error_label = QtWidgets.QLabel(f"Error al cargar módulo Caducidad:\n{e}")
            error_layout = QtWidgets.QVBoxLayout(self.caducidad_tab)
            error_layout.addWidget(error_label, alignment=QtCore.Qt.AlignCenter)
            self.inner_tab.addTab(self.caducidad_tab, "Caducidad por PO [Error]")
        # -----------------------------------------

        main_layout.addWidget(self.inner_tab)

    def setup_catalog_ui(self, parent_widget):
        layout = QtWidgets.QVBoxLayout(parent_widget)
        # --- Barra de búsqueda ---
        search_layout = QtWidgets.QHBoxLayout()
        self.search_field = QtWidgets.QLineEdit()
        self.search_field.setPlaceholderText("Buscar por ItemCode o Descripción")
        self.search_field.returnPressed.connect(self.apply_catalog_filter) # Cambiado nombre
        search_layout.addWidget(self.search_field)

        self.search_button = QtWidgets.QPushButton("Buscar")
        self.search_button.clicked.connect(self.apply_catalog_filter) # Cambiado nombre
        search_layout.addWidget(self.search_button)

        self.refresh_button = QtWidgets.QPushButton("Refrescar")
        self.refresh_button.clicked.connect(self.refresh_catalog) # Cambiado nombre
        search_layout.addWidget(self.refresh_button)
        layout.addLayout(search_layout)

        # Autocompletado
        self.completer = QtWidgets.QCompleter()
        self.completer.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
        self.completer.setFilterMode(QtCore.Qt.MatchContains)
        self.search_field.setCompleter(self.completer)

        # --- Tabla ---
        self.catalog_table = QtWidgets.QTableWidget() # Cambiado nombre de variable
        self.catalog_table.setColumnCount(3)
        # Asegúrate que los nombres coincidan con tu Excel (ItemCode, Description, Foto Local)
        self.catalog_table.setHorizontalHeaderLabels(["ItemCode", "Description", "Foto"])
        self.catalog_table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        self.catalog_table.horizontalHeader().setSectionResizeMode(2, QtWidgets.QHeaderView.ResizeToContents) # Ajustar columna foto
        self.catalog_table.setIconSize(QtCore.QSize(150, 150))
        self.catalog_table.verticalHeader().setDefaultSectionSize(150)
        self.catalog_table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers) # No editable directamente
        self.catalog_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.catalog_table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        layout.addWidget(self.catalog_table)

        # Conectar doble clic
        self.catalog_table.cellDoubleClicked.connect(self.open_catalog_item_details) # Cambiado nombre

        # --- Botón de Agregar Item ---
        self.btn_agregar = QtWidgets.QPushButton("Agregar Item")
        self.btn_agregar.clicked.connect(self.agregar_catalog_item) # Cambiado nombre
        # Habilitar/deshabilitar según permiso
        self.btn_agregar.setEnabled(check_update_permission(Session.user_alias))
        layout.addWidget(self.btn_agregar, alignment=QtCore.Qt.AlignRight)

        # Cargar datos iniciales del catálogo
        self.load_catalog_data() # Llamar a la carga de datos específica del catálogo

    def load_catalog_data(self): # Cambiado nombre
        """Carga los datos desde el archivo Excel del catálogo."""
        print(f"Cargando catálogo desde: {CATALOGO_PATH}")
        if not os.path.exists(CATALOGO_PATH):
             QtWidgets.QMessageBox.critical(self, "Error", f"El archivo del catálogo no se encuentra:\n{CATALOGO_PATH}")
             self.df = pd.DataFrame(columns=["ItemCode", "Description", "Foto", "Foto Local"]) # Crear DF vacío
             self.populate_catalog_table(self.df) # Limpiar tabla
             return
        try:
            # Leer el Excel, asegurando que las columnas de fotos se lean como texto
            # Especificar dtype para columnas clave al leer
            dtype_spec = {'ItemCode': str, 'Description': str, 'Foto': str, 'Foto Local': str}
            self.df = pd.read_excel(CATALOGO_PATH, dtype=dtype_spec)

            # Rellenar NaNs en columnas clave DESPUÉS de leer, si aún existen
            for col in dtype_spec.keys():
                 if col in self.df.columns:
                     self.df[col] = self.df[col].fillna('').astype(str)
                 else:
                     print(f"Advertencia: Columna '{col}' esperada no encontrada en {CATALOGO_PATH}")
                     self.df[col] = '' # Añadir columna vacía si falta

            print(f"Catálogo cargado. {len(self.df)} filas.")
            # print(self.df.head()) # Descomentar para depurar las primeras filas
        except FileNotFoundError:
             QtWidgets.QMessageBox.critical(self, "Error", f"No se encontró el archivo del catálogo:\n{CATALOGO_PATH}")
             self.df = pd.DataFrame(columns=["ItemCode", "Description", "Foto", "Foto Local"])
        except Exception as e:
             QtWidgets.QMessageBox.critical(self, "Error", f"Error al cargar el catálogo '{CATALOGO_PATH}':\n{e}")
             traceback.print_exc()
             self.df = pd.DataFrame(columns=["ItemCode", "Description", "Foto", "Foto Local"])

        # Actualizar autocompletado
        try:
            suggestions = list(set(
                self.df["ItemCode"].dropna().astype(str).tolist() +
                self.df["Description"].dropna().astype(str).tolist()
            ))
            # Quitar sugerencias vacías si existen
            suggestions = [s for s in suggestions if s]
            if self.completer.model():
                self.completer.model().setStringList(suggestions)
            else:
                self.completer.setModel(QtCore.QStringListModel(suggestions))
        except KeyError as e:
             print(f"Advertencia: Columna {e} no encontrada para autocompletado de catálogo.")
        except Exception as e:
             print(f"Error actualizando autocompletado de catálogo: {e}")


        # Poblar la tabla
        self.populate_catalog_table(self.df) # Cambiado nombre

    def populate_catalog_table(self, df_to_show): # Cambiado nombre
        """Llena la tabla del catálogo con los datos del DataFrame, priorizando imagen local."""
        self.catalog_table.setRowCount(0) # Limpiar tabla
        self.catalog_table.setSortingEnabled(False) # Deshabilitar ordenamiento mientras se llena

        # Guardar índices originales del DataFrame completo (self.df)
        # Mapear índices del df_to_show (filtrado) a los índices originales
        original_indices = df_to_show.index

        # Iterar usando los índices originales para acceder a self.df si es necesario
        # pero usar df_to_show para los datos a mostrar
        for i, original_index in enumerate(original_indices):
            row = df_to_show.loc[original_index] # Obtener datos de la fila filtrada
            row_position = self.catalog_table.rowCount()
            self.catalog_table.insertRow(row_position)

            # Columna 0: ItemCode
            itemcode = str(row.get("ItemCode", ""))
            itemcode_item = QtWidgets.QTableWidgetItem(itemcode)
            self.catalog_table.setItem(row_position, 0, itemcode_item)

            # Columna 1: Description
            description = str(row.get("Description", ""))
            desc_item = QtWidgets.QTableWidgetItem(description)
            self.catalog_table.setItem(row_position, 1, desc_item)

            # Columna 2: Foto (Label con imagen)
            photo_label = QtWidgets.QLabel()
            photo_label.setFixedSize(140, 140) # Tamaño fijo para la celda
            photo_label.setAlignment(QtCore.Qt.AlignCenter)
            photo_label.setStyleSheet("border: 1px solid #ddd;") # Estilo

            # Intentar cargar imagen local primero
            local_path = str(row.get("Foto Local", ""))
            network_path = str(row.get("Foto", "")) # Fallback
            image_loaded = False

            if local_path and local_path.lower() != 'nan' and os.path.isfile(local_path):
                pixmap = QtGui.QPixmap(local_path)
                if not pixmap.isNull():
                    pixmap = pixmap.scaled(140, 140, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
                    photo_label.setPixmap(pixmap)
                    image_loaded = True
                # else: print(f"Pixmap local nulo para {local_path}")

            # Si no se cargó local, intentar red
            if not image_loaded and network_path and network_path.lower() != 'nan' and os.path.isfile(network_path):
                pixmap = QtGui.QPixmap(network_path)
                if not pixmap.isNull():
                    pixmap = pixmap.scaled(140, 140, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
                    photo_label.setPixmap(pixmap)
                    image_loaded = True
                    # Podríamos intentar copiarla localmente aquí si falta? O mejor en la sync.
                # else: print(f"Pixmap red nulo para {network_path}")

            if not image_loaded:
                photo_label.setText("No Image")

            # Contenedor para centrar el QLabel en la celda
            container = QtWidgets.QWidget()
            layout = QtWidgets.QHBoxLayout(container)
            layout.setContentsMargins(0, 0, 0, 0)
            layout.setAlignment(QtCore.Qt.AlignCenter)
            layout.addWidget(photo_label)
            self.catalog_table.setCellWidget(row_position, 2, container)

            # Guardar el índice original del DataFrame COMPLETO (self.df) en el item de la fila
            # Usamos UserRole para almacenar datos personalizados
            for col in range(self.catalog_table.columnCount()):
                 item = self.catalog_table.item(row_position, col)
                 if item is None: # Para la columna de la imagen (widget)
                     item = QtWidgets.QTableWidgetItem() # Crear item temporal si no existe
                     self.catalog_table.setItem(row_position, col, item)
                 item.setData(QtCore.Qt.UserRole, original_index) # Guardar índice original de self.df

        self.catalog_table.resizeRowsToContents()
        self.catalog_table.setSortingEnabled(True) # Habilitar ordenamiento

    def apply_catalog_filter(self): # Cambiado nombre
        """Filtra la tabla del catálogo según el texto en el campo de búsqueda."""
        text = self.search_field.text().strip()
        if not text: # Si no hay texto, mostrar todo
            filtered_df = self.df
        else:
            # Filtrar por ItemCode O Description (insensible a mayúsculas/minúsculas)
            try:
                # Asegurarse que las columnas son string antes de filtrar
                itemcode_str = self.df["ItemCode"].astype(str)
                desc_str = self.df["Description"].astype(str)
                condition = (
                    itemcode_str.str.contains(text, case=False, na=False) |
                    desc_str.str.contains(text, case=False, na=False)
                )
                filtered_df = self.df[condition]
            except Exception as e:
                print(f"Error durante el filtrado del catálogo: {e}")
                traceback.print_exc()
                filtered_df = self.df # Mostrar todo si hay error en filtro
        self.populate_catalog_table(filtered_df) # Cambiado nombre

    def refresh_catalog(self): # Cambiado nombre
        """Recarga los datos del catálogo y limpia el filtro."""
        print("Refrescando catálogo...")
        self.search_field.clear()
        self.load_catalog_data() # Vuelve a cargar desde el Excel y puebla la tabla

    def agregar_catalog_item(self): # Cambiado nombre
        """Abre el diálogo para agregar un nuevo item al catálogo y lo procesa."""
        # La comprobación de permisos ya se hizo en el botón
        dialog = AddItemDialog(self)
        if dialog.exec_() == QtWidgets.QDialog.Accepted:
            data = dialog.getData()
            itemcode = data["ItemCode"] # Ya viene en mayúsculas desde el diálogo
            description = data["Description"]
            orig_image_path = data["SelectedImagePath"] # Ruta original de la imagen seleccionada

            # Verificar duplicado de ItemCode (insensible a mayúsculas/minúsculas)
            if not self.df.empty and itemcode.lower() in self.df["ItemCode"].astype(str).str.lower().values:
                QtWidgets.QMessageBox.warning(
                    self, "Error",
                    f"El ItemCode '{itemcode}' ya existe en el catálogo. No se puede agregar duplicado."
                )
                return

            # --- Procesamiento de la imagen ---
            dest_local_path = ""
            dest_network_path = ""
            if orig_image_path: # Solo procesar si se seleccionó una imagen
                try:
                    # Crear nombres y rutas de destino
                    _, ext = os.path.splitext(orig_image_path)
                    if not ext: ext = ".png" # Default a png
                    dest_filename = f"{itemcode}{ext.lower()}"

                    dest_local_path = os.path.join(LOCAL_IMAGENES_CAT_PATH, dest_filename)
                    dest_network_path = os.path.join(IMAGENES_CAT_PATH, dest_filename)

                    # Copiar a local
                    os.makedirs(LOCAL_IMAGENES_CAT_PATH, exist_ok=True)
                    shutil.copy2(orig_image_path, dest_local_path)
                    print(f"Imagen para nuevo item copiada a local: {dest_local_path}")

                    # Copiar a red
                    try:
                        os.makedirs(IMAGENES_CAT_PATH, exist_ok=True)
                        shutil.copy2(orig_image_path, dest_network_path)
                        print(f"Imagen para nuevo item copiada a red: {dest_network_path}")
                    except Exception as e_net:
                        print(f"Error al copiar imagen a red: {e_net}")
                        QtWidgets.QMessageBox.warning(self, "Error de Red", f"No se pudo copiar la imagen a la carpeta de red:\n{e_net}\nEl item se guardará con la imagen local, pero puede no ser visible para otros.")
                        # No abortamos, guardamos con la ruta de red esperada aunque no se copiara

                except Exception as e_img:
                    QtWidgets.QMessageBox.critical(self, "Error de Imagen", f"No se pudo procesar o copiar la imagen:\n{e_img}")
                    # Decidir si abortar o continuar sin imagen
                    dest_local_path = "" # Asegurar rutas vacías si falla la copia
                    dest_network_path = ""
                    # return # Descomentar si se requiere abortar si falla la imagen

            # --- Agregar al DataFrame y guardar Excel ---
            # Crear diccionario para la nueva fila, asegurando que todas las columnas existan
            new_row_data = {col: '' for col in self.df.columns} # Inicializar con valores vacíos
            new_row_data.update({
                "ItemCode": itemcode,
                "Description": description,
                "Foto": dest_network_path, # Guardar ruta de red (puede estar vacía)
                "Foto Local": dest_local_path # Guardar ruta local (puede estar vacía)
            })
            new_row = pd.DataFrame([new_row_data], columns=self.df.columns) # Asegurar orden de columnas

            # Concatenar la nueva fila al DataFrame existente
            # Usar pd.concat en lugar de append (deprecated)
            self.df = pd.concat([self.df, new_row], ignore_index=True)

            try:
                # Guardar el DataFrame completo de vuelta al Excel
                # --- Considerar File Lock ---
                # lock_path = CATALOGO_PATH + ".lock"
                # with FileLock(lock_path):
                self.df.to_excel(CATALOGO_PATH, index=False, engine='openpyxl')
                # --- Fin File Lock ---
                print("Catálogo guardado en Excel exitosamente.")
                QtWidgets.QMessageBox.information(self, "Éxito", f"Nuevo Item '{itemcode}' agregado al catálogo.")
                # Refrescar la tabla para mostrar el nuevo item (aplicando filtro actual)
                self.apply_catalog_filter()
                # Actualizar autocompletado
                suggestions = list(set(
                    self.df["ItemCode"].dropna().astype(str).tolist() +
                    self.df["Description"].dropna().astype(str).tolist()
                ))
                suggestions = [s for s in suggestions if s] # Quitar vacíos
                if self.completer.model():
                    self.completer.model().setStringList(suggestions)
                else:
                    self.completer.setModel(QtCore.QStringListModel(suggestions))

            except Exception as e_save:
                QtWidgets.QMessageBox.critical(self, "Error al Guardar", f"No se pudo guardar el catálogo en '{CATALOGO_PATH}':\n{e_save}\nEl nuevo item no se ha guardado permanentemente.")
                traceback.print_exc()
                # Opcional: revertir la adición al DataFrame en memoria
                self.df = self.df[:-1] # Eliminar la última fila añadida
                self.apply_catalog_filter() # Refrescar tabla sin el item
                return


    def open_catalog_item_details(self, row, column): # Cambiado nombre
        """Abre el diálogo de detalles para el item seleccionado en el catálogo."""
        try:
            # Obtener el índice original del DataFrame completo (self.df) almacenado en el item
            item = self.catalog_table.item(row, 0) # Usar la primera columna como referencia
            if item is None: return # No hacer nada si el item no existe
            original_df_index = item.data(QtCore.Qt.UserRole)
            if original_df_index is None or original_df_index not in self.df.index:
                 QMessageBox.warning(self, "Error", "Índice de fila no válido o desactualizado.")
                 return

            # Obtener los datos de esa fila del DataFrame original (self.df)
            item_data = self.df.loc[original_df_index].to_dict()

            # Crear y mostrar el diálogo
            dialog = ItemDetailsDialog(item_data, Session.user_alias, parent=self) # Pasar alias actual
            if dialog.exec_() == QtWidgets.QDialog.Accepted:
                updated_data = dialog.getData()
                print(f"Datos actualizados recibidos del diálogo: {updated_data}")

                # Actualizar la fila correspondiente en el DataFrame (self.df)
                update_success = False
                for key, val in updated_data.items():
                    if key in self.df.columns:
                        self.df.loc[original_df_index, key] = val
                        update_success = True
                    else:
                        print(f"Advertencia: La columna '{key}' del diálogo no existe en el DataFrame principal del catálogo.")

                if update_success:
                    # Guardar el DataFrame modificado en el archivo Excel
                    try:
                         # --- Considerar File Lock ---
                         # lock_path = CATALOGO_PATH + ".lock"
                         # with FileLock(lock_path):
                         self.df.to_excel(CATALOGO_PATH, index=False, engine='openpyxl')
                         # --- Fin File Lock ---
                         print("Catálogo guardado en Excel después de modificar item.")
                         # Refrescar la tabla para mostrar los cambios (reaplicando filtro)
                         self.apply_catalog_filter()
                    except Exception as e:
                         QtWidgets.QMessageBox.critical(self, "Error al Guardar", f"No se pudo guardar el catálogo:\n{e}")
                         traceback.print_exc()
                         # Considerar recargar los datos desde el archivo si falla el guardado
                         # self.load_catalog_data()
                else:
                     print("No se realizaron cambios en el catálogo para guardar.")

        except Exception as e:
             print(f"Error al abrir detalles del item del catálogo: {e}")
             traceback.print_exc()
             QtWidgets.QMessageBox.warning(self, "Error", f"No se pudieron abrir los detalles del item seleccionado.\n{e}")

# =============================================================================
# Diálogo para Detalles del Herramental (Edición)
# =============================================================================
class HerramentalDetailsDialog(QtWidgets.QDialog):
    """
    Diálogo para ver y editar de forma ampliada la información de un herramental.
    Se muestran los datos en un formulario; al presionar "Editar" se habilitan los campos.
    Ahora se incluye también un botón “Eliminar” que, de confirmarse, marcará el registro para eliminación.
    """
    def __init__(self, item_data, user_alias, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Detalle del Herramental")
        self.resize(600, 400)
        self.item_data = item_data.copy()  # Copia de los datos de la fila
        self.user_alias = user_alias
        self.edit_mode = False
        self.deleted = False  # Bandera para indicar eliminación
        
        # Guardamos la nomenclatura original para identificar el registro
        self.original_nomenclatura = self.item_data.get("NOMENCLATURA", "")

        main_layout = QtWidgets.QVBoxLayout(self)
        form_layout = QtWidgets.QFormLayout()
        
        self.txt_proyecto = QtWidgets.QLineEdit(str(self.item_data.get("PROYECTO", "")))
        self.txt_proyecto.setReadOnly(True)
        form_layout.addRow("PROYECTO:", self.txt_proyecto)

        self.txt_nomenclatura = QtWidgets.QLineEdit(str(self.item_data.get("NOMENCLATURA", "")))
        self.txt_nomenclatura.setReadOnly(True)
        form_layout.addRow("NOMENCLATURA:", self.txt_nomenclatura)

        self.txt_modelo = QtWidgets.QLineEdit(str(self.item_data.get("MODELO", "")))
        self.txt_modelo.setReadOnly(True)
        form_layout.addRow("MODELO:", self.txt_modelo)

        self.txt_job = QtWidgets.QLineEdit(str(self.item_data.get("JOB", "")))
        self.txt_job.setReadOnly(True)
        form_layout.addRow("JOB:", self.txt_job)

        self.txt_tipo = QtWidgets.QLineEdit(str(self.item_data.get("TIPO DE HERRAMENTAL", "")))
        self.txt_tipo.setReadOnly(True)
        form_layout.addRow("TIPO DE HERRAMENTAL:", self.txt_tipo)

        self.txt_rack = QtWidgets.QLineEdit(str(self.item_data.get("RACK", "")))
        self.txt_rack.setReadOnly(True)
        form_layout.addRow("RACK:", self.txt_rack)

        # STATUS_INOUT se muestra mediante un ComboBox
        self.cmb_status = QtWidgets.QComboBox()
        self.cmb_status.addItems(["in", "out", "area roja", "scrap"])
        current_status = str(self.item_data.get("STATUS_INOUT", "")).lower()
        index = self.cmb_status.findText(current_status, QtCore.Qt.MatchFixedString)
        if index >= 0:
            self.cmb_status.setCurrentIndex(index)
        else:
            self.cmb_status.setCurrentIndex(0)
        self.cmb_status.setEnabled(False)
        form_layout.addRow("ESTADO DE SURTIDO:", self.cmb_status)

        main_layout.addLayout(form_layout)

        # Botón Box: inicialmente se muestra solo "Close" y el botón "Editar"
        self.button_box = QtWidgets.QDialogButtonBox()
        self.btn_editar = QtWidgets.QPushButton("Editar")
        self.btn_editar.clicked.connect(self.editar)
        self.button_box.addButton(self.btn_editar, QtWidgets.QDialogButtonBox.ActionRole)
        close_button = self.button_box.addButton(QtWidgets.QDialogButtonBox.Close)
        close_button.clicked.connect(self.reject)
        main_layout.addWidget(self.button_box)

        # Botón "Eliminar" (oculto hasta validar permisos y entrar en modo edición)
        self.btn_eliminar = QtWidgets.QPushButton("Eliminar")
        self.btn_eliminar.setVisible(False)
        self.btn_eliminar.clicked.connect(self.eliminar)
        main_layout.addWidget(self.btn_eliminar)

    def editar(self):
        # Aquí se deben validar permisos (por ejemplo, si Session.user_alias y check_update_permission() son válidos)
        self.txt_proyecto.setReadOnly(False)
        self.txt_nomenclatura.setReadOnly(False)
        self.txt_modelo.setReadOnly(False)
        self.txt_job.setReadOnly(False)
        self.txt_tipo.setReadOnly(False)
        self.txt_rack.setReadOnly(False)
        self.cmb_status.setEnabled(True)
        self.edit_mode = True
        
        # Cambiar "Editar" por "Guardar" y habilitar el botón "Eliminar"
        self.btn_editar.setText("Guardar")
        self.btn_editar.clicked.disconnect()
        self.btn_editar.clicked.connect(self.guardar_cambios)
        self.btn_eliminar.setVisible(True)

    def guardar_cambios(self):
        self.item_data["PROYECTO"] = self.txt_proyecto.text().strip()
        self.item_data["NOMENCLATURA"] = self.txt_nomenclatura.text().strip()
        self.item_data["MODELO"] = self.txt_modelo.text().strip()
        self.item_data["JOB"] = self.txt_job.text().strip()
        self.item_data["TIPO DE HERRAMENTAL"] = self.txt_tipo.text().strip()
        self.item_data["RACK"] = self.txt_rack.text().strip()
        self.item_data["STATUS_INOUT"] = self.cmb_status.currentText()
        self.item_data["NOMENCLATURA_ANTIGUA"] = self.original_nomenclatura
        self.accept()

    def eliminar(self):
        # Confirmación de eliminación
        confirm = QtWidgets.QMessageBox.question(
            self,
            "Confirmar eliminación",
            "¿Estás seguro de eliminar este herramental?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No
        )
        if confirm == QtWidgets.QMessageBox.Yes:
            self.deleted = True
            self.accept()

    def getData(self):
        return self.item_data
# =============================================================================
# Diálogo para Agregar un nuevo Herramental
# =============================================================================
class AgregarHerramentalDialog(QtWidgets.QDialog):
    """
    Diálogo para agregar un nuevo registro de herramental.
    Se muestran los campos disponibles para ingreso, en modo edición inmediata.
    Para el campo 'TIPO DE HERRAMENTAL' se despliega un ComboBox con opciones predefinidas;
    si se selecciona 'OTRO', se solicita ingreso manual.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Agregar Herramental")
        self.resize(600, 400)
        self.item_data = {}
        
        main_layout = QtWidgets.QVBoxLayout(self)
        form_layout = QtWidgets.QFormLayout()
        
        # --- ComboBox para PROYECTO ---
        self.cmb_proyecto = QtWidgets.QComboBox()
        self.cmb_proyecto.setEditable(True)
        # Se intenta leer el CSV para obtener la lista de proyectos
        if os.path.exists(DB_PATH):
            try:
                df = pd.read_csv(DB_PATH, encoding="utf-8-sig")
                proyectos = df["PROYECTO"].dropna().unique().tolist()
                # Ordena la lista si hace falta
                proyectos.sort()
                self.cmb_proyecto.addItems(proyectos)
            except Exception as e:
                QtWidgets.QMessageBox.warning(self, "Advertencia", f"No se pudo cargar la lista de proyectos:\n{e}")
        form_layout.addRow("PROYECTO:", self.cmb_proyecto)

        self.txt_nomenclatura = QtWidgets.QLineEdit()
        form_layout.addRow("NOMENCLATURA:", self.txt_nomenclatura)
        
        self.txt_modelo = QtWidgets.QLineEdit()
        form_layout.addRow("MODELO:", self.txt_modelo)
        
        self.txt_job = QtWidgets.QLineEdit()
        form_layout.addRow("JOB:", self.txt_job)
        
        # Combobox para TIPO DE HERRAMENTAL con opción OTRO
        self.cmb_tipo = QtWidgets.QComboBox()
        opciones_tipo = ["PALLET", "STENCIL", "WORK HOLDER", "PLATO ROUTER", "FIXTURA", 
                         "DUMMY", "ACRILICO", "PERFILADORA", "CONSUMIBLE", 
                         "PALLET DE OLA", "PALLET DE PRESSFIT", "PALLET DE SMT", "PALLET POLYMERICS", "SQUEGEE", "OTRO"]
        self.cmb_tipo.addItems(opciones_tipo)
        self.cmb_tipo.currentIndexChanged.connect(self.verificar_otro_tipo)
        form_layout.addRow("TIPO DE HERRAMENTAL:", self.cmb_tipo)

        self.txt_rack = QtWidgets.QLineEdit()
        form_layout.addRow("RACK:", self.txt_rack)
        
        # Combobox para STATUS_INOUT
        self.cmb_status = QtWidgets.QComboBox()
        self.cmb_status.addItems(["in", "out", "area roja", "scrap"])
        form_layout.addRow("ESTADO DE SURTIDO:", self.cmb_status)
        
        main_layout.addLayout(form_layout)
        
        # Botones Aceptar/Cancelar
        button_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.guardar)
        button_box.rejected.connect(self.reject)
        main_layout.addWidget(button_box)
    
    def verificar_otro_tipo(self, index):
        # Si se selecciona "OTRO", solicita al usuario el ingreso manual
        if self.cmb_tipo.itemText(index) == "OTRO":
            otro, ok = QtWidgets.QInputDialog.getText(self, "Tipo de Herramental", "Ingrese otro tipo:")
            if ok and otro.strip():
                # Reemplaza el valor de "OTRO" por el ingresado
                self.cmb_tipo.setItemText(index, otro.strip())
    
    def guardar(self):
        # Obtener el valor ingresado o seleccionado en el combo de proyecto
        self.item_data["PROYECTO"] = self.cmb_proyecto.currentText().strip()
        self.item_data["NOMENCLATURA"] = self.txt_nomenclatura.text().strip()
        self.item_data["MODELO"] = self.txt_modelo.text().strip()
        self.item_data["JOB"] = self.txt_job.text().strip()
        self.item_data["TIPO DE HERRAMENTAL"] = self.cmb_tipo.currentText().strip()
        self.item_data["RACK"] = self.txt_rack.text().strip()
        self.item_data["STATUS_INOUT"] = self.cmb_status.currentText()
        self.accept()
    
    def getData(self):
        return self.item_data

# =============================================================================
# Segunda pestaña: Inventario de Herramentales
# =============================================================================
class InventarioHerramentalesPage(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.user_alias = ""  # Debe asignarse, ej: "USUARIO1"
        self.full_df = pd.DataFrame()  # DataFrame completo
        self.df = pd.DataFrame()       # DataFrame filtrado
        self.column_filters = {}       # Filtros por columna
        self.initUI()
        self.load_data()

    def initUI(self):
        main_layout = QtWidgets.QVBoxLayout(self)

        # --- Barra de búsqueda global y botones ---
        search_layout = QtWidgets.QHBoxLayout()
        self.search_field = QtWidgets.QLineEdit()
        self.search_field.setPlaceholderText("Buscar por NOMENCLATURA o JOB")
        self.search_field.returnPressed.connect(self.apply_all_filters)
        search_layout.addWidget(self.search_field)
        
        self.search_button = QtWidgets.QPushButton("Buscar")
        self.search_button.clicked.connect(self.apply_all_filters)
        search_layout.addWidget(self.search_button)
        
        self.refresh_button = QtWidgets.QPushButton("Refresh")
        self.refresh_button.clicked.connect(self.refresh)
        search_layout.addWidget(self.refresh_button)
        
        # Botón para agregar un nuevo herramental
        self.agregar_button = QtWidgets.QPushButton("Agregar Herramental")
        self.agregar_button.clicked.connect(self.agregar_herramental)
        search_layout.addWidget(self.agregar_button)
        
        main_layout.addLayout(search_layout)

        # --- Tabla para mostrar el inventario ---
        self.table = QtWidgets.QTableWidget()
        self.table.setColumnCount(7)
        headers = ["PROYECTO", "NOMENCLATURA", "MODELO", "JOB", "TIPO DE HERRAMENTAL", "RACK", "ESTADO DE SURTIDO"]
        self.table.setHorizontalHeaderLabels(headers)
        # Configuración de redimensión:
        self.table.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)       # PROYECTO
        self.table.horizontalHeader().setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeToContents) # NOMENCLATURA
        self.table.horizontalHeader().setSectionResizeMode(2, QtWidgets.QHeaderView.Stretch)       # MODELO
        self.table.horizontalHeader().setSectionResizeMode(3, QtWidgets.QHeaderView.ResizeToContents) # JOB
        self.table.horizontalHeader().setSectionResizeMode(4, QtWidgets.QHeaderView.Stretch)       # TIPO DE HERRAMENTAL
        self.table.horizontalHeader().setSectionResizeMode(5, QtWidgets.QHeaderView.ResizeToContents) # RACK
        self.table.horizontalHeader().setSectionResizeMode(6, QtWidgets.QHeaderView.Stretch)       # ESTADO DE SURTIDO
        self.table.verticalHeader().setDefaultSectionSize(40)
        self.table.horizontalHeader().sectionClicked.connect(self.on_header_clicked)
        main_layout.addWidget(self.table)

        # --- Botón para exportar inventario ---
        self.export_button = QtWidgets.QPushButton("Exportar Inventario de Herramentales")
        self.export_button.clicked.connect(self.export_inventory)
        main_layout.addWidget(self.export_button)

        # Conectar doble clic en la tabla para editar detalles
        self.table.cellDoubleClicked.connect(self.open_item_details)

    def load_data(self):
        try:
            df = pd.read_csv(DB_PATH, encoding="utf-8-sig")
            # Remover posibles caracteres BOM en los nombres de las columnas:
            df.columns = df.columns.str.strip('ï»¿')
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Error al cargar inventario de herramentales:\n{e}")
            return
        # Excluir registros cuyo "TIPO DE HERRAMENTAL" contenga "CONSUMABLE"
        if "TIPO DE HERRAMENTAL" in df.columns:
            df = df[~df["TIPO DE HERRAMENTAL"].astype(str).str.contains("CONSUMABLE", case=False, na=False)]
        self.full_df = df.copy()
        self.column_filters = {}
        self.apply_all_filters()

        # Actualizar autocompletado basado en NOMENCLATURA y JOB
        suggestions = list(set(
            self.full_df["NOMENCLATURA"].dropna().astype(str).tolist() +
            self.full_df["JOB"].dropna().astype(str).tolist()
        ))
        if self.completer_exists():
            self.search_field.completer().model().setStringList(suggestions)
        else:
            comp_model = QtCore.QStringListModel(suggestions)
            completer = QtWidgets.QCompleter()
            completer.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
            completer.setFilterMode(QtCore.Qt.MatchContains)
            completer.setModel(comp_model)
            self.search_field.setCompleter(completer)

    def completer_exists(self):
        return self.search_field.completer() is not None

    def apply_all_filters(self):
        df_filtered = self.full_df.copy()
        global_filter = self.search_field.text().strip()
        if global_filter:
            condition = (df_filtered["NOMENCLATURA"].astype(str).str.contains(global_filter, case=False, na=False) |
                         df_filtered["JOB"].astype(str).str.contains(global_filter, case=False, na=False))
            df_filtered = df_filtered[condition]
        for col, filt in self.column_filters.items():
            if filt:
                if col == "ESTADO DE SURTIDO":
                    df_filtered = df_filtered[df_filtered["STATUS_INOUT"].astype(str).str.contains(filt, case=False, na=False)]
                else:
                    df_filtered = df_filtered[df_filtered[col].astype(str).str.contains(filt, case=False, na=False)]
        self.df = df_filtered.copy()
        self.populate_table(self.df)

    def populate_table(self, df):
        self.table.setRowCount(0)
        for index, row in df.iterrows():
            row_pos = self.table.rowCount()
            self.table.insertRow(row_pos)
            self.table.setItem(row_pos, 0, QtWidgets.QTableWidgetItem(str(row.get("PROYECTO", ""))))
            self.table.setItem(row_pos, 1, QtWidgets.QTableWidgetItem(str(row.get("NOMENCLATURA", ""))))
            self.table.setItem(row_pos, 2, QtWidgets.QTableWidgetItem(str(row.get("MODELO", ""))))
            self.table.setItem(row_pos, 3, QtWidgets.QTableWidgetItem(str(row.get("JOB", ""))))
            self.table.setItem(row_pos, 4, QtWidgets.QTableWidgetItem(str(row.get("TIPO DE HERRAMENTAL", ""))))
            self.table.setItem(row_pos, 5, QtWidgets.QTableWidgetItem(str(row.get("RACK", ""))))
            self.table.setItem(row_pos, 6, QtWidgets.QTableWidgetItem(str(row.get("STATUS_INOUT", ""))))

    def on_header_clicked(self, logicalIndex):
        headers = ["PROYECTO", "NOMENCLATURA", "MODELO", "JOB", "TIPO DE HERRAMENTAL", "RACK", "ESTADO DE SURTIDO"]
        header_name = headers[logicalIndex]
        if header_name in {"PROYECTO", "TIPO DE HERRAMENTAL", "ESTADO DE SURTIDO"}:
            source_col = "STATUS_INOUT" if header_name == "ESTADO DE SURTIDO" else header_name
            suggestions = sorted(list(self.full_df[source_col].dropna().unique()))
            suggestions.insert(0, "")
            current_filter = self.column_filters.get(header_name, "")
            current_index = suggestions.index(current_filter) if current_filter in suggestions else 0
            item, ok = QtWidgets.QInputDialog.getItem(self, f"Filtrar por {header_name}",
                                                      f"Seleccione filtro para {header_name}:",
                                                      suggestions, current_index, False)
            if ok:
                self.column_filters[header_name] = item
                self.apply_all_filters()
        else:
            current_filter = self.column_filters.get(header_name, "")
            text, ok = QtWidgets.QInputDialog.getText(self, f"Filtrar por {header_name}",
                                                      f"Ingrese filtro para {header_name}:", text=current_filter)
            if ok:
                self.column_filters[header_name] = text.strip()
                self.apply_all_filters()

    def apply_filter(self):
        self.apply_all_filters()

    def refresh(self):
        self.search_field.clear()
        self.column_filters = {}
        self.load_data()

    def open_item_details(self, row, column):
        item_data = self.df.iloc[row].to_dict()
        dialog = HerramentalDetailsDialog(item_data, self.user_alias, parent=self)
        if dialog.exec_() == QtWidgets.QDialog.Accepted:
            if getattr(dialog, "deleted", False):
                # Verificar que la columna HERRAMENTAL_ID exista
                if "HERRAMENTAL_ID" not in self.full_df.columns:
                    QtWidgets.QMessageBox.critical(self, "Error", "La columna HERRAMENTAL_ID no existe en el DataFrame.")
                    return
                # Eliminar el registro cuyo valor de NOMENCLATURA coincide con el original
                self.full_df = self.full_df[self.full_df["NOMENCLATURA"] != dialog.original_nomenclatura].copy()
                # Reasignar de forma secuencial la columna HERRAMENTAL_ID
                self.full_df.sort_values(by="HERRAMENTAL_ID", inplace=True)
                self.full_df["HERRAMENTAL_ID"] = range(1, len(self.full_df) + 1)
            else:
                updated_data = dialog.getData()
                original_nomenclatura = updated_data.get("NOMENCLATURA_ANTIGUA", item_data.get("NOMENCLATURA"))
                df_index = self.full_df.index[self.full_df["NOMENCLATURA"] == original_nomenclatura]
                if not df_index.empty:
                    self.full_df.loc[df_index[0], :] = updated_data
                else:
                    QtWidgets.QMessageBox.warning(self, "Aviso", "No se encontró el registro a actualizar.")
            try:
                self.full_df.to_csv(DB_PATH, index=False, encoding="utf-8-sig")
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo guardar el inventario:\n{e}")
            self.apply_all_filters()


    def agregar_herramental(self):
        dialog = AgregarHerramentalDialog(parent=self)
        if dialog.exec_() == QtWidgets.QDialog.Accepted:
            new_item = dialog.getData()
            
            # Definir la ruta del bloqueo a partir de DB_PATH
            lock_path = DB_PATH + ".lock"
            lock = FileLock(lock_path)
            
            with lock:  # Este bloque asegura que la escritura sea exclusiva
                # Releer el CSV para capturar cambios realizados por otros usuarios
                if os.path.exists(DB_PATH):
                    self.full_df = pd.read_csv(DB_PATH, encoding="utf-8-sig")
                else:
                    self.full_df = pd.DataFrame()

                # Asignar consecutivo para HERRAMENTAL_ID basado en la versión actualizada del DataFrame
                if not self.full_df.empty:
                    try:
                        max_id = self.full_df["HERRAMENTAL_ID"].max()
                        new_id = int(max_id) + 1
                    except Exception:
                        new_id = 1
                else:
                    new_id = 1
                new_item["HERRAMENTAL_ID"] = new_id

                # Forzar que TYPE_INOUT sea "SINGLE"
                new_item["TYPE_INOUT"] = "SINGLE"

                # Asignar valor para MPI
                new_item["MPI"] = r"\\gdlnt104\LABELCONFIG\LABELS\B18\TOOL\ToolTrack+\MPI\MPI-280-INFRAESTRUCTURA BI-011-C Mantenimiento e Identificacion de Herramentales.pdf"

                # Completar con columnas adicionales que existen en DB_PATH pero no están en el diálogo
                columnas_adicionales = [
                    "ITEM_TYPE", "PROCESO", "ULTIMO_MANTENIMIENTO", "PROXIMO_MANTENIMIENTO", 
                    "PERIODO", "STATUS", "DIAS_ALERTA", "USER_OUT", 
                    "MULTI_STOCK_IN", "MULTI_STOCK_OUT", "MULTI_STOCK_ALL", "LAST_OUT", "EMPLOYEE_OUT", 
                    "USER_LAST_MAINTENANCE", "LADO", "TYPE_CONS_INOUT", "TICKNESS", "is_update", "MOV"
                ]
                for col in columnas_adicionales:
                    if col not in new_item:
                        new_item[col] = ""

                # Agregar el nuevo registro al DataFrame y escribirlo
                self.full_df = pd.concat([self.full_df, pd.DataFrame([new_item])], ignore_index=True)
                try:
                    self.full_df.to_csv(DB_PATH, index=False, encoding="utf-8-sig")
                except Exception as e:
                    QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo guardar el inventario:\n{e}")

            # Carga los datos actualizados una vez liberado el bloqueo
            self.load_data()

    def export_inventory(self):
        msg_box = QtWidgets.QMessageBox(self)
        msg_box.setWindowTitle("Exportar Inventario")
        msg_box.setText("Seleccione la opción de exportación:")
        btn_all = msg_box.addButton("Exportar Todo", QtWidgets.QMessageBox.AcceptRole)
        btn_filtered = msg_box.addButton("Exportar Filtrado", QtWidgets.QMessageBox.RejectRole)
        msg_box.exec_()
        if msg_box.clickedButton() == btn_all:
            data_to_export = self.full_df
        else:
            data_to_export = self.df

        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
        export_filename = os.path.join(desktop_path, "Inventario_Herramentales.xlsx")
        try:
            data_to_export.to_excel(export_filename, index=False, engine="openpyxl")
            QtWidgets.QMessageBox.information(self, "Exportar", f"Inventario exportado a:\n{export_filename}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo exportar el inventario:\n{e}")
       
# --- Nombres de Columnas (Consistente con tu definición) ---
COL_CONSUMIBLE = "Consumible"
COL_CUSTOMER = "Customer"
COL_PN_MANUFACTURABLE = "PN Manufacturable" # O "PN_Manufacturable"
COL_W1 = "W1"
COL_W2 = "W2"
COL_WASTE = "Waste"
COL_FU_UNIDAD = "FU_Unidad"
COL_UOM = "UOM_Consumible"
COL_EDIFICIO = "EDIFICIO"
COL_PROJECT = "PROJECT"
COL_FORECAST_QTY = "Quantity" # Asume que esta es la columna de cantidad en FORECAST.csv
COL_FORECAST_PN = "PN Manufacturable" # Asume que esta es la columna PN en FORECAST.csv
COL_INV_ITEM = "Item" # Asume que esta es la columna del item en SKID_TOOLTRACK+.csv (o el nombre correcto)
COL_INV_ON_HAND = "On Hand" # Asume columna en SKID_TOOLTRACK+.csv
COL_INV_ON_ORDER = "On Order" # Asume columna en SKID_TOOLTRACK+.csv
COL_INV_FISICO = "Inventario Fisico" # Nueva columna a gestionar
COL_INV_SAFETY_STOCK = "Safety Stock" # Nueva columna a gestionar

# --- Funciones Utilitarias (sin cambios) ---
def safe_float_conversion(value, default=0.0):
    """Convierte a float de forma segura, devolviendo un valor por defecto en caso de error."""
    try:
        if isinstance(value, str):
            value = value.replace(',', '.')
        return float(value)
    except (ValueError, TypeError):
        return default

def find_column_name(df_columns, potential_names):
    """Encuentra el nombre real de una columna en una lista de nombres potenciales."""
    cleaned_df_columns = [str(col).strip().lstrip('ï»¿') for col in df_columns] # Limpiar nombres para comparar
    for name in potential_names:
        clean_name = str(name).strip() # Limpiar nombre potencial
        if clean_name in cleaned_df_columns:
            try:
                original_index = cleaned_df_columns.index(clean_name)
                return df_columns[original_index] # Devuelve el nombre original
            except ValueError:
                pass # Continuar buscando
    # Fallback si no se encuentra
    print(f"Advertencia: Ninguno de los nombres {potential_names} encontrado en las columnas {df_columns}. Usando '{potential_names[0]}' como fallback.")
    return potential_names[0]

def clean_bom(text):
    """Elimina el BOM UTF-8 (ï»¿) del inicio de un string si está presente."""
    bom = 'ï»¿'
    if isinstance(text, str) and text.startswith(bom):
        return text[len(bom):]
    return text

# --- Delegado Numérico (sin cambios) ---
class NumericDelegate(QtWidgets.QStyledItemDelegate):
    """Permite la edición de números (flotantes) directamente en una celda de QTableWidget."""
    def createEditor(self, parent, option, index):
        editor = QtWidgets.QLineEdit(parent)
        validator = QtGui.QDoubleValidator(0.0, 1000000.0, 5, editor) # Ajusta rango/precisión
        validator.setNotation(QtGui.QDoubleValidator.StandardNotation)
        editor.setValidator(validator)
        return editor

    def setEditorData(self, editor, index):
        value = index.model().data(index, QtCore.Qt.EditRole)
        if value is None or pd.isna(value):
            editor.setText("")
        else:
            editor.setText(str(value))

    def setModelData(self, editor, model, index):
        value_str = editor.text().strip().replace(',', '.')
        try:
            float_value = float(value_str)
            model.setData(index, float_value, QtCore.Qt.EditRole)
        except ValueError:
            print(f"Advertencia: No se pudo convertir '{value_str}' a float en la celda ({index.row()}, {index.column()}). Guardando como string.")
            model.setData(index, value_str, QtCore.Qt.EditRole) # O manejar diferente

    def updateEditorGeometry(self, editor, option, index):
        editor.setGeometry(option.rect)


# --- Diálogo Parámetros (sin cambios funcionales mayores, pero considera UOM) ---
class ParametrosDialog(QtWidgets.QDialog):
    """
    Diálogo para Añadir/Actualizar Parámetros de FU.
    NOTA: Este diálogo NO implementa la lógica de cálculo específica por tipo (Flux, Hilo, etc.).
    Asume que el usuario introduce W1/W2 para Pasta/Barra O el FU_Unidad directo para otros tipos.
    La lista de UOM debería actualizarse para incluir 'gal', 'pzas', 'kg', 'lt'.
    """
    def __init__(self, params_csv_path, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Añadir/Actualizar Parámetros de FU")
        self.params_csv_path = params_csv_path
        self.setModal(True)

        layout = QtWidgets.QFormLayout(self)
        layout.setSpacing(10)

        # --- Campos de Entrada ---
        self.txt_consumible = QtWidgets.QLineEdit()
        self.txt_customer = QtWidgets.QLineEdit()
        self.txt_pn = QtWidgets.QLineEdit()
        self.txt_w1 = QtWidgets.QLineEdit()
        self.txt_w2 = QtWidgets.QLineEdit()
        self.txt_fu_unidad = QtWidgets.QLineEdit()
        self.txt_waste = QtWidgets.QLineEdit()

        # *** IMPORTANTE: Actualizar lista de UOM ***
        self.cmb_uom = QtWidgets.QComboBox()
        self.cmb_uom.addItems(["g", "kg", "gal", "lt", "pzas", "ea"]) # Añadir unidades relevantes

        # Tooltips (sin cambios)
        self.txt_consumible.setToolTip("Nombre o código del consumible.")
        # ... (otros tooltips) ...
        self.cmb_uom.setToolTip("Unidad de medida del consumible (g, kg, gal, lt, pzas, ea).")

        # Validadores (sin cambios)
        double_validator = QtGui.QDoubleValidator(0.0, 1000000.0, 5) # Ampliado rango y precisión
        double_validator.setNotation(QtGui.QDoubleValidator.StandardNotation)
        self.txt_w1.setValidator(double_validator)
        self.txt_w2.setValidator(double_validator)
        self.txt_waste.setValidator(double_validator)
        self.txt_fu_unidad.setValidator(double_validator)

        # --- Añadir Widgets al Layout (sin cambios) ---
        layout.addRow(f"{COL_CONSUMIBLE}:", self.txt_consumible)
        # ... (otros rows) ...
        pn_label = find_column_name([COL_PN_MANUFACTURABLE, "PN_Manufacturable"], [COL_PN_MANUFACTURABLE, "PN_Manufacturable"])
        layout.addRow(f"{pn_label}:", self.txt_pn)
        layout.addRow(f"{COL_W1} (Pasta/Barra):", self.txt_w1)
        layout.addRow(f"{COL_W2} (Pasta/Barra):", self.txt_w2)
        layout.addRow(f"{COL_FU_UNIDAD} (Otros tipos):", self.txt_fu_unidad)
        layout.addRow(f"{COL_WASTE} (ej., 0.05):", self.txt_waste)
        layout.addRow(f"{COL_UOM}:", self.cmb_uom)


        # --- Botones (sin cambios) ---
        self.buttonBox = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Save | QtWidgets.QDialogButtonBox.Cancel)
        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)
        layout.addRow(self.buttonBox)

    def get_data(self):
        # --- Recuperación de datos (sin cambios lógicos importantes aquí) ---
        # La lógica sigue priorizando W1/W2 si existen para calcular FU,
        # o usa el FU_Unidad ingresado. La UOM es crucial.
        consumible = self.txt_consumible.text().strip()
        customer = self.txt_customer.text().strip()
        pn = self.txt_pn.text().strip()
        w1_text = self.txt_w1.text().strip().replace(',', '.')
        w2_text = self.txt_w2.text().strip().replace(',', '.')
        waste_text = self.txt_waste.text().strip().replace(',', '.')
        fu_unidad_text = self.txt_fu_unidad.text().strip().replace(',', '.')
        uom = self.cmb_uom.currentText().strip()

        if not all([consumible, customer, pn, waste_text, uom]):
             QtWidgets.QMessageBox.warning(self, "Advertencia", "Los campos Consumible, Customer, PN, Waste y UOM son obligatorios.")
             return None

        w1 = safe_float_conversion(w1_text, None)
        w2 = safe_float_conversion(w2_text, None)
        waste = safe_float_conversion(waste_text, None)
        fu_unidad = safe_float_conversion(fu_unidad_text, None)

        calculated_fu = None
        # Validar W1/W2 sólo si la UOM sugiere que son relevantes (ej. 'g')
        uom_lower = uom.lower()
        if uom_lower in ['g', 'gr', 'kg']: # Ajusta si W1/W2 aplican a otras unidades
             if w1 is not None and w2 is not None:
                 if w2 < w1:
                     QtWidgets.QMessageBox.warning(self, "Advertencia", "W2 no puede ser menor que W1.")
                     return None
                 calculated_fu = w2 - w1
                 if fu_unidad is not None:
                      QtWidgets.QMessageBox.information(self, "Información", f"Se usarán W1 y W2 para calcular el FU ({calculated_fu:.5f} {uom}). El valor de FU Unidad ({fu_unidad}) será ignorado para el cálculo base.")
                 # Usar el FU calculado desde W1/W2 como el FU base para este tipo
                 fu_unidad = calculated_fu
             elif fu_unidad is None: # Si es basado en peso pero no se dieron W1/W2 ni FU directo
                  QtWidgets.QMessageBox.warning(self, "Advertencia", f"Para UOM '{uom}', debe proporcionar W1 y W2 válidos, o un valor directo para FU Unidad.")
                  return None
        # Para otras UOM (gal, pzas, etc.) o si no se usan W1/W2
        if calculated_fu is None: # Si no se calculó desde W1/W2
            if fu_unidad is None:
                 QtWidgets.QMessageBox.warning(self, "Advertencia", "Debe proporcionar un valor para FU Unidad.")
                 return None
            elif fu_unidad < 0:
                 QtWidgets.QMessageBox.warning(self, "Advertencia", "FU Unidad no puede ser negativo.")
                 return None
            # Se usará el fu_unidad ingresado directamente

        if waste is None or waste < 0:
            QtWidgets.QMessageBox.warning(self, "Advertencia", "El valor de Waste debe ser un número positivo (ej. 0.05).")
            return None

        return {
            COL_CONSUMIBLE: consumible,
            COL_CUSTOMER: customer,
            COL_PN_MANUFACTURABLE: pn,
            COL_W1: w1 if w1 is not None else pd.NA,
            COL_W2: w2 if w2 is not None else pd.NA,
            COL_WASTE: waste,
            COL_FU_UNIDAD: fu_unidad, # FU Base (calculado de W1/W2 o directo)
            COL_UOM: uom
        }

    def accept(self):
        # --- Guardado (sin cambios lógicos importantes) ---
        # Sigue guardando los valores recuperados por get_data
        data = self.get_data()
        if data is None:
            return

        try:
            df = pd.DataFrame()
            pn_key = COL_PN_MANUFACTURABLE # Default key name
            expected_cols = [COL_CONSUMIBLE, COL_CUSTOMER, pn_key, COL_W1, COL_W2, COL_WASTE, COL_FU_UNIDAD, COL_UOM]

            if os.path.exists(self.params_csv_path):
                try:
                    df = pd.read_csv(self.params_csv_path, encoding="utf-8-sig")
                    original_columns = list(df.columns)
                    df.columns = [col.strip().lstrip('ï»¿') for col in original_columns]
                    pn_key = find_column_name(df.columns, [COL_PN_MANUFACTURABLE, "PN_Manufacturable"])
                    expected_cols = [COL_CONSUMIBLE, COL_CUSTOMER, pn_key, COL_W1, COL_W2, COL_WASTE, COL_FU_UNIDAD, COL_UOM] # Update expected cols order/name
                except Exception as read_err:
                    QtWidgets.QMessageBox.critical(self, "Error de Lectura", f"No se pudo leer {os.path.basename(self.params_csv_path)}:\n{read_err}")
                    return
            else:
                 # Create empty df with standard cols if file doesn't exist
                 df = pd.DataFrame(columns=expected_cols)

            # Ensure all expected columns exist
            for col in expected_cols:
                if col not in df.columns:
                    print(f"Advertencia (accept): Columna '{col}' no encontrada. Añadiendo.")
                    df[col] = pd.NA

            # Find if record exists
            condition = (
                (df[COL_CONSUMIBLE].astype(str).str.strip().str.lower() == data[COL_CONSUMIBLE].lower()) &
                (df[COL_CUSTOMER].astype(str).str.strip().str.lower() == data[COL_CUSTOMER].lower()) &
                (df[pn_key].astype(str).str.strip().str.lower() == data[COL_PN_MANUFACTURABLE].lower())
            )
            condition = condition.fillna(False)

            if condition.any():
                # Update existing
                idx = df.index[condition][0]
                df.loc[idx, COL_W1] = data[COL_W1]
                df.loc[idx, COL_W2] = data[COL_W2]
                df.loc[idx, COL_WASTE] = data[COL_WASTE]
                df.loc[idx, COL_FU_UNIDAD] = data[COL_FU_UNIDAD]
                df.loc[idx, COL_UOM] = data[COL_UOM]
            else:
                # Add new record
                new_record_dict = {
                    COL_CONSUMIBLE: data[COL_CONSUMIBLE],
                    COL_CUSTOMER: data[COL_CUSTOMER],
                    pn_key: data[COL_PN_MANUFACTURABLE], # Use detected PN key
                    COL_W1: data[COL_W1],
                    COL_W2: data[COL_W2],
                    COL_WASTE: data[COL_WASTE],
                    COL_FU_UNIDAD: data[COL_FU_UNIDAD],
                    COL_UOM: data[COL_UOM]
                 }
                # Append as a new row using concat
                new_record_df = pd.DataFrame([new_record_dict])
                df = pd.concat([df, new_record_df], ignore_index=True)


            # Save updated DataFrame
            df_to_save = df[expected_cols] # Ensure correct column order/names
            df_to_save.to_csv(self.params_csv_path, index=False, encoding="utf-8-sig")
            QtWidgets.QMessageBox.information(self, "Éxito", f"Parámetros guardados en\n{os.path.basename(self.params_csv_path)}")
            super().accept() # Close dialog

        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Ocurrió un error al guardar:\n{e}")
            traceback.print_exc()


# --- Pestaña Factor de Uso (MODIFICADA) ---
class TabFactorUso(QtWidgets.QWidget):
    """Pestaña para visualizar y gestionar los parámetros del Factor de Uso."""
    def __init__(self, params_file_path, parent=None):
        super().__init__(parent)
        self.params_file = params_file_path
        self.df_params = pd.DataFrame() # Datos originales leídos
        self.df_display = pd.DataFrame()# Datos procesados para mostrar

        layout = QtWidgets.QVBoxLayout(self)

        # --- Controles Superiores (Sin cambios) ---
        top_layout = QtWidgets.QHBoxLayout()
        self.search_edit = QtWidgets.QLineEdit()
        self.search_edit.setPlaceholderText("Buscar Consumible, Cliente o PN...")
        self.search_edit.textChanged.connect(self.filter_data)
        self.btn_add_update = QtWidgets.QPushButton("Añadir/Actualizar Parámetro")
        self.btn_add_update.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogNewFolder))
        self.btn_add_update.clicked.connect(self.open_params_dialog)
        self.btn_refresh = QtWidgets.QPushButton("Refrescar Datos")
        self.btn_refresh.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_BrowserReload))
        self.btn_refresh.clicked.connect(self.load_data)
        top_layout.addWidget(QtWidgets.QLabel("Buscar:"))
        top_layout.addWidget(self.search_edit)
        top_layout.addWidget(self.btn_add_update)
        top_layout.addWidget(self.btn_refresh)
        layout.addLayout(top_layout)

        # --- Tabla de Parámetros (Sin cambios) ---
        self.table = QtWidgets.QTableWidget()
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table)

        self.load_data() # Carga inicial

    # *** MÉTODO load_data MODIFICADO ***
    def load_data(self):
        """Carga y procesa los datos desde el archivo CSV de parámetros."""
        try:
            if not os.path.exists(self.params_file):
                QtWidgets.QMessageBox.warning(self, "Archivo no encontrado", f"El archivo {os.path.basename(self.params_file)} no se encontró.")
                # Define columnas estándar esperadas para DataFrame vacío
                pn_key = find_column_name([], [COL_PN_MANUFACTURABLE, "PN_Manufacturable"])
                empty_cols = [COL_CONSUMIBLE, COL_CUSTOMER, pn_key, COL_W1, COL_W2, COL_WASTE, COL_FU_UNIDAD, COL_UOM, "FU_Calculado", "FU_Total"]
                self.df_params = pd.DataFrame(columns=empty_cols)
                self.df_display = self.df_params.copy()
                self.display_data(self.df_display) # Mostrar tabla vacía
                return

            self.df_params = pd.read_csv(self.params_file, encoding="utf-8-sig")
            original_columns = list(self.df_params.columns)
            self.df_params.columns = [col.strip().lstrip('ï»¿') for col in original_columns]
            print(f"Columnas leídas de {os.path.basename(self.params_file)}: {original_columns}")
            print(f"Columnas limpiadas: {list(self.df_params.columns)}")

            # --- Identificar nombres reales de columnas ---
            pn_key = find_column_name(self.df_params.columns, [COL_PN_MANUFACTURABLE, "PN_Manufacturable"])
            w1_key = find_column_name(self.df_params.columns, [COL_W1])
            w2_key = find_column_name(self.df_params.columns, [COL_W2])
            fu_unidad_key = find_column_name(self.df_params.columns, [COL_FU_UNIDAD])
            waste_key = find_column_name(self.df_params.columns, [COL_WASTE])
            uom_key = find_column_name(self.df_params.columns, [COL_UOM])
            # customer_key = find_column_name(self.df_params.columns, [COL_CUSTOMER])
            # consumible_key = find_column_name(self.df_params.columns, [COL_CONSUMIBLE])

            # --- Asegurar tipos numéricos ---
            num_cols_keys = [w1_key, w2_key, fu_unidad_key, waste_key]
            for col_key in num_cols_keys:
                if col_key in self.df_params.columns:
                    # Guardar original por si falla conversión
                    # original_dtype = self.df_params[col_key].dtype
                    # print(f"Converting column '{col_key}' from {original_dtype}")
                    # Convertir a string, reemplazar comas, luego a numérico
                    self.df_params[col_key] = pd.to_numeric(
                        self.df_params[col_key].astype(str).str.replace(',', '.', regex=False),
                        errors='coerce' # Pone NaN si no se puede convertir
                    )
                    # print(f"  Resulting type: {self.df_params[col_key].dtype}")
                else:
                    print(f"Advertencia: Columna '{col_key}' no encontrada. Se añadirá como NA.")
                    self.df_params[col_key] = pd.NA

            # Asegurar que UOM sea string
            if uom_key in self.df_params.columns:
                 self.df_params[uom_key] = self.df_params[uom_key].astype(str).fillna('')
            else:
                 print(f"Advertencia: Columna UOM '{uom_key}' no encontrada. Se añadirá vacía.")
                 self.df_params[uom_key] = ''


            # --- Calcular FU_Calculado (Factor de Uso Base) ---
            print("Calculando FU_Calculado (base)...")
            # Inicializar columna
            self.df_params['FU_Calculado'] = np.nan

            # Condición para cálculo W2-W1 (solo si UOM es g/gr y W1/W2 son válidos)
            # Usar .str.lower() para comparación insensible a mayúsculas
            # Asegurarse que las columnas existan antes de usarlas
            if all(k in self.df_params.columns for k in [uom_key, w1_key, w2_key]):
                is_weight_based = (
                    self.df_params[uom_key].str.lower().isin(['g', 'gr']) &
                    self.df_params[w1_key].notna() &
                    self.df_params[w2_key].notna() &
                   (self.df_params[w2_key] >= self.df_params[w1_key]) # Asegurar W2 >= W1
                )
                print(f"  {is_weight_based.sum()} rows identified for W2-W1 calculation.")
                # Calcular W2-W1 donde la condición es verdadera
                self.df_params['FU_Calculado'] = np.where(
                    is_weight_based,
                    self.df_params[w2_key] - self.df_params[w1_key],
                    self.df_params['FU_Calculado'] # Mantener NaN donde no aplica W2-W1
                )
            else:
                print("  Advertencia: Columnas W1, W2 o UOM faltantes, no se puede calcular FU basado en peso.")
                is_weight_based = pd.Series(False, index=self.df_params.index) # Serie de Falsos


            # Para el resto (donde no aplicó W2-W1), usar FU_Unidad directamente
            # Asegurarse que fu_unidad_key existe
            if fu_unidad_key in self.df_params.columns:
                 # Rellenar FU_Calculado con FU_Unidad donde FU_Calculado aún es NaN
                 self.df_params['FU_Calculado'] = self.df_params['FU_Calculado'].fillna(self.df_params[fu_unidad_key])
                 print(f"  Usando FU_Unidad para {(~is_weight_based).sum()} filas restantes.")
            else:
                 print(f"  Advertencia: Columna FU_Unidad '{fu_unidad_key}' faltante.")
                 # Si FU_Unidad falta, FU_Calculado permanecerá NaN para esas filas


            # Convertir FU_Calculado a numérico final (por si acaso FU_Unidad era string)
            self.df_params['FU_Calculado'] = pd.to_numeric(self.df_params['FU_Calculado'], errors='coerce')

            # --- Calcular FU_Total (aplicando Waste) ---
            print("Calculando FU_Total (con Waste)...")
            # Asegurar que waste_key existe
            if waste_key in self.df_params.columns:
                 fu_base_num = self.df_params['FU_Calculado'].fillna(0) # Usar 0 si el FU base es NaN
                 waste_num = self.df_params[waste_key].fillna(0)       # Usar 0 si Waste es NaN
                 self.df_params['FU_Total'] = fu_base_num * (1 + waste_num)
            else:
                 print(f"  Advertencia: Columna Waste '{waste_key}' no encontrada. FU_Total será igual a FU_Calculado.")
                 self.df_params['FU_Total'] = self.df_params['FU_Calculado']


            # --- Preparar para Display ---
            # Llenar NaN restantes con string vacío para visualización
            self.df_display = self.df_params.fillna('')
            print("Cálculos de FU completados para visualización.")

            self.filter_data() # Aplicar filtro/búsqueda actual

        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error de Carga/Proceso", f"Error al cargar o procesar {os.path.basename(self.params_file)}:\n{e}")
            traceback.print_exc()
            self.df_params = pd.DataFrame()
            self.df_display = pd.DataFrame()
            self.display_data(self.df_display) # Mostrar tabla vacía

    # --- filter_data, display_data, open_params_dialog, reset_search (sin cambios) ---
    def filter_data(self):
        """Filtra los datos mostrados en la tabla según el texto de búsqueda."""
        df_to_filter = self.df_display.copy()
        search_term = self.search_edit.text().strip().lower()

        if not search_term:
            df_filtered = df_to_filter
        else:
            # Identificar columnas de búsqueda (usando nombres estándar ya en df_display)
            pn_key = find_column_name(df_to_filter.columns, [COL_PN_MANUFACTURABLE, "PN_Manufacturable"])
            customer_key = find_column_name(df_to_filter.columns, [COL_CUSTOMER])
            consumible_key = find_column_name(df_to_filter.columns, [COL_CONSUMIBLE])

            search_cols = [col for col in [consumible_key, customer_key, pn_key] if col in df_to_filter.columns]

            if not search_cols:
                QtWidgets.QMessageBox.warning(self, "Error de Filtro", "No se encontraron las columnas para buscar.")
                df_filtered = df_to_filter
            else:
                condition = pd.Series([False] * len(df_to_filter))
                for col in search_cols:
                    try: # Añadir try-except por si la columna existe pero tiene tipos mixtos inesperados
                        condition |= df_to_filter[col].astype(str).str.lower().str.contains(search_term, na=False)
                    except Exception as filter_err:
                        print(f"Error al filtrar columna '{col}': {filter_err}")
                df_filtered = df_to_filter[condition]

        self.display_data(df_filtered)

    def display_data(self, df):
        """Muestra el DataFrame en el QTableWidget."""
        self.table.clearContents()
        self.table.setRowCount(0)

        if df.empty:
            # Opcional: Limpiar headers si no hay datos
            # self.table.setColumnCount(0)
            return

        # --- Definir columnas a mostrar (usando nombres estándar) ---
        # Usar las columnas presentes en el df que se va a mostrar (df_display filtrado)
        # Incluir las calculadas ('FU_Calculado', 'FU_Total')
        display_columns_keys = list(df.columns) # Usar todas las columnas del df filtrado

        # Configurar headers si es necesario
        if self.table.columnCount() != len(display_columns_keys):
            self.table.setColumnCount(len(display_columns_keys))
            # Headers deberían ser los nombres estándar/calculados limpios
            # Intentar mapear las claves limpias a nombres más legibles si es necesario
            header_map = { # Mapa de nombres limpios a nombres deseados en header
                 "PN_Manufacturable": COL_PN_MANUFACTURABLE, # Mostrar nombre con espacio
                 "FU_Unidad": COL_FU_UNIDAD,
                 "UOM_Consumible": COL_UOM,
                 "FU_Calculado": "FU Calculado", # Nombres más legibles
                 "FU_Total": "FU Total"
            }
            final_headers = [header_map.get(col, col) for col in display_columns_keys] # Usar mapa o el nombre limpio
            self.table.setHorizontalHeaderLabels(final_headers)

        self.table.setRowCount(len(df))

        for i, (_, row) in enumerate(df.iterrows()):
            for j, col_key in enumerate(display_columns_keys):
                item_value = row[col_key]
                # Formatear números
                try_float = None
                # Intentar convertir sólo si no es ya un número (evita errores con tipos mixtos)
                if not isinstance(item_value, (int, float, np.number)):
                     try_float = safe_float_conversion(item_value, None)
                else:
                     try_float = float(item_value) # Es número, convertir a float estándar

                if try_float is not None and not pd.isna(try_float): # Chequear NaN explícitamente
                    item_text = f"{try_float:.5f}" # 5 decimales para FU
                    alignment = QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter
                else:
                    item_text = str(item_value) # Mostrar como string si no es numérico o es NaN
                    alignment = QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter

                table_item = QtWidgets.QTableWidgetItem(item_text)
                table_item.setTextAlignment(alignment)
                self.table.setItem(i, j, table_item)

        self.table.resizeColumnsToContents()
        if self.table.columnCount() > 0:
             try: # Ajustar ancho, manejar posible error si no hay items
                  self.table.horizontalHeader().setStretchLastSection(True)
             except Exception as e:
                  print(f"Error ajustando última sección: {e}")

    def open_params_dialog(self):
        """Abre el diálogo para añadir o actualizar parámetros."""
        dialog = ParametrosDialog(self.params_file, parent=self)
        if dialog.exec() == QtWidgets.QDialog.Accepted: # Usar exec() en lugar de exec_() para PySide6
            self.load_data() # Recargar datos si se guardaron cambios

    def reset_search(self):
        """Limpia la búsqueda y recarga todos los datos."""
        self.search_edit.clear()
        # self.load_data() # filter_data se llama automáticamente al limpiar


# --- Pestaña Forecast (sin cambios) ---
class TabForecast(QtWidgets.QWidget):
    """Pestaña para visualizar los datos del Forecast."""
    def __init__(self, forecast_file_path, parent=None):
        super().__init__(parent)
        self.forecast_file = forecast_file_path
        self.df_forecast = pd.DataFrame()
        self.df_display = pd.DataFrame()

        layout = QtWidgets.QVBoxLayout(self)

        # --- Controles de Filtrado ---
        filter_layout = QtWidgets.QHBoxLayout()
        self.cmb_edificio = QtWidgets.QComboBox()
        self.cmb_proyecto = QtWidgets.QComboBox()
        self.btn_refresh = QtWidgets.QPushButton("Refrescar Datos")
        self.btn_refresh.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_BrowserReload))

        filter_layout.addWidget(QtWidgets.QLabel("Edificio:"))
        filter_layout.addWidget(self.cmb_edificio)
        filter_layout.addWidget(QtWidgets.QLabel("Proyecto:"))
        filter_layout.addWidget(self.cmb_proyecto)
        filter_layout.addStretch()
        filter_layout.addWidget(self.btn_refresh)
        layout.addLayout(filter_layout)

        # --- Tabla de Forecast ---
        self.table = QtWidgets.QTableWidget()
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table)

        # --- Conexiones ---
        self.cmb_edificio.currentIndexChanged.connect(self.filter_data)
        self.cmb_proyecto.currentIndexChanged.connect(self.filter_data)
        self.btn_refresh.clicked.connect(self.load_data)

        self.load_data() # Carga inicial

    def load_data(self):
        """Carga los datos del archivo CSV de forecast."""
        try:
            if not os.path.exists(self.forecast_file):
                QtWidgets.QMessageBox.warning(self, "Archivo no encontrado", f"El archivo {os.path.basename(self.forecast_file)} no se encontró.")
                self.df_forecast = pd.DataFrame()
                self.populate_filters()
                self.display_data(self.df_forecast)
                return

            self.df_forecast = pd.read_csv(self.forecast_file, encoding="utf-8-sig")
            original_columns = list(self.df_forecast.columns)
            self.df_forecast.columns = [col.strip().lstrip('ï»¿') for col in original_columns]
            print(f"Forecast: Columnas limpiadas: {list(self.df_forecast.columns)}")

            self.df_forecast.fillna('', inplace=True) # Llenar NaNs
            self.populate_filters()
            self.filter_data()

        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error de Carga", f"Error al cargar {os.path.basename(self.forecast_file)}:\n{e}")
            traceback.print_exc()
            self.df_forecast = pd.DataFrame()
            self.populate_filters()
            self.display_data(self.df_forecast)

    def populate_filters(self):
        """Llena los ComboBoxes de Edificio y Proyecto."""
        # Desconectar señales
        try: self.cmb_edificio.currentIndexChanged.disconnect(self.filter_data)
        except: pass
        try: self.cmb_proyecto.currentIndexChanged.disconnect(self.filter_data)
        except: pass

        self.cmb_edificio.clear()
        self.cmb_proyecto.clear()
        self.cmb_edificio.addItem("Todos")
        self.cmb_proyecto.addItem("Todos")

        if not self.df_forecast.empty:
            edificio_key = find_column_name(self.df_forecast.columns, [COL_EDIFICIO, 'Building']) # Añadir alias
            project_key = find_column_name(self.df_forecast.columns, [COL_PROJECT, 'Project']) # Añadir alias

            if edificio_key in self.df_forecast.columns:
                edificios = sorted([str(x) for x in self.df_forecast[edificio_key].unique() if pd.notna(x) and str(x).strip()])
                self.cmb_edificio.addItems(edificios)
            if project_key in self.df_forecast.columns:
                proyectos = sorted([str(x) for x in self.df_forecast[project_key].unique() if pd.notna(x) and str(x).strip()])
                self.cmb_proyecto.addItems(proyectos)

        # Reconectar señales
        self.cmb_edificio.currentIndexChanged.connect(self.filter_data)
        self.cmb_proyecto.currentIndexChanged.connect(self.filter_data)

    def filter_data(self):
        """Filtra el DataFrame principal."""
        if self.df_forecast.empty:
            self.display_data(self.df_forecast)
            return

        df = self.df_forecast.copy()
        selected_edificio = self.cmb_edificio.currentText()
        selected_proyecto = self.cmb_proyecto.currentText()

        edificio_key = find_column_name(df.columns, [COL_EDIFICIO, 'Building'])
        project_key = find_column_name(df.columns, [COL_PROJECT, 'Project'])

        if selected_edificio != "Todos" and edificio_key in df.columns:
            df = df[df[edificio_key].astype(str) == selected_edificio]
        if selected_proyecto != "Todos" and project_key in df.columns:
            df = df[df[project_key].astype(str) == selected_proyecto]

        self.df_display = df
        self.display_data(self.df_display)

    def display_data(self, df):
        """Muestra el DataFrame en la tabla."""
        self.table.clearContents()
        self.table.setRowCount(0)

        if df.empty:
            self.table.setColumnCount(0)
            return

        headers = list(df.columns)
        if self.table.columnCount() != len(headers):
            self.table.setColumnCount(len(headers))
            self.table.setHorizontalHeaderLabels(headers)

        self.table.setRowCount(len(df))
        for i, (_, row) in enumerate(df.iterrows()):
            for j, col in enumerate(headers):
                item_value = row[col]
                item_text = str(item_value)
                table_item = QtWidgets.QTableWidgetItem(item_text)
                # Alinear números a la derecha
                if isinstance(item_value, (int, float, np.number)):
                     table_item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                elif item_text.replace('.', '', 1).replace(',', '', 1).strip().lstrip('-').isdigit(): # Detectar numérico como string
                     table_item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)

                self.table.setItem(i, j, table_item)

        self.table.resizeColumnsToContents()
        if self.table.columnCount() > 0:
             try:
                  self.table.horizontalHeader().setStretchLastSection(True)
             except Exception as e:
                  print(f"Error ajustando última sección (Forecast): {e}")


# --- Pestaña Demanda (MODIFICADA) ---
class TabDemanda(QtWidgets.QWidget):
    """Pestaña para calcular y visualizar la demanda de consumibles y gestionar inventario."""
    def __init__(self, params_file, forecast_file, inventory_file, parent=None):
        super().__init__(parent)
        self.params_file = params_file
        self.forecast_file = forecast_file
        self.inventory_file = inventory_file

        # DataFrames
        self.df_params = pd.DataFrame()
        self.df_forecast = pd.DataFrame()
        self.df_inventory = pd.DataFrame()
        self.df_demand = pd.DataFrame() # DataFrame final con cálculos

        layout = QtWidgets.QVBoxLayout(self)

        # --- Controles Superiores (Sin cambios) ---
        top_layout = QtWidgets.QHBoxLayout()
        self.btn_calculate = QtWidgets.QPushButton("Calcular Demanda y Estado Inventario")
        self.btn_calculate.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_MediaPlay))
        self.btn_save_inventory = QtWidgets.QPushButton("Guardar Cambios Inventario")
        self.btn_save_inventory.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_DialogSaveButton))
        self.btn_save_inventory.setEnabled(False)
        top_layout.addWidget(self.btn_calculate)
        top_layout.addStretch()
        top_layout.addWidget(self.btn_save_inventory)
        layout.addLayout(top_layout)

        # --- Tabla de Demanda (Sin cambios) ---
        self.table = QtWidgets.QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table)

        # --- Conexiones (Sin cambios) ---
        self.btn_calculate.clicked.connect(self.calculate_demand)
        self.btn_save_inventory.clicked.connect(self.save_inventory_changes)
        self.table.itemChanged.connect(self.on_item_changed)

        # --- Inicialización ---
        self.load_initial_data() # Cargar datos base

    # *** MÉTODO load_initial_data MODIFICADO ***
    def load_initial_data(self):
        """Carga y procesa los datos necesarios de los archivos CSV."""
        print("Cargando datos iniciales para el Panel de Demanda...")
        self.df_params = pd.DataFrame()
        self.df_forecast = pd.DataFrame()
        self.df_inventory = pd.DataFrame()

        # --- Cargar y Procesar Parámetros (FU_DB_PATH) ---
        try:
            if os.path.exists(self.params_file):
                self.df_params = pd.read_csv(self.params_file, encoding="utf-8-sig")
                original_columns = list(self.df_params.columns)
                self.df_params.columns = [col.strip().lstrip('ï»¿') for col in original_columns]
                print(f"Parámetros cargados: {len(self.df_params)} filas. Cols limpias: {list(self.df_params.columns)}")

                # --- Identificar columnas ---
                w1_key = find_column_name(self.df_params.columns, [COL_W1])
                w2_key = find_column_name(self.df_params.columns, [COL_W2])
                fu_unidad_key = find_column_name(self.df_params.columns, [COL_FU_UNIDAD])
                waste_key = find_column_name(self.df_params.columns, [COL_WASTE])
                uom_key = find_column_name(self.df_params.columns, [COL_UOM])

                # --- Asegurar tipos numéricos ---
                num_cols_keys = [w1_key, w2_key, fu_unidad_key, waste_key]
                for col_key in num_cols_keys:
                    if col_key in self.df_params.columns:
                        self.df_params[col_key] = pd.to_numeric(
                            self.df_params[col_key].astype(str).str.replace(',', '.', regex=False),
                            errors='coerce'
                        )
                    else:
                        self.df_params[col_key] = pd.NA # Añadir si falta

                # Asegurar que UOM sea string
                if uom_key in self.df_params.columns:
                    self.df_params[uom_key] = self.df_params[uom_key].astype(str).fillna('')
                else:
                    self.df_params[uom_key] = '' # Añadir si falta

                # --- Calcular FU_Calculado (Factor de Uso Base) ---
                print("Calculando FU_Calculado (base) para demanda...")
                self.df_params['FU_Calculado'] = np.nan # Inicializar
                if all(k in self.df_params.columns for k in [uom_key, w1_key, w2_key]):
                     is_weight_based = (
                         self.df_params[uom_key].str.lower().isin(['g', 'gr']) &
                         self.df_params[w1_key].notna() &
                         self.df_params[w2_key].notna() &
                        (self.df_params[w2_key] >= self.df_params[w1_key])
                     )
                     self.df_params['FU_Calculado'] = np.where(
                         is_weight_based,
                         self.df_params[w2_key] - self.df_params[w1_key],
                         self.df_params['FU_Calculado'] # Mantener NaN donde no aplica
                     )
                else:
                     is_weight_based = pd.Series(False, index=self.df_params.index) # Track para el else

                # Usar FU_Unidad para el resto
                if fu_unidad_key in self.df_params.columns:
                     self.df_params['FU_Calculado'] = self.df_params['FU_Calculado'].fillna(self.df_params[fu_unidad_key])
                # Convertir a numérico final
                self.df_params['FU_Calculado'] = pd.to_numeric(self.df_params['FU_Calculado'], errors='coerce')

                # --- Calcular FU_Total (aplicando Waste) ---
                print("Calculando FU_Total (con Waste) para demanda...")
                if waste_key in self.df_params.columns:
                     fu_base_num = self.df_params['FU_Calculado'].fillna(0)
                     waste_num = self.df_params[waste_key].fillna(0)
                     self.df_params['FU_Total'] = fu_base_num * (1 + waste_num)
                else:
                     print(f"  Advertencia: Columna Waste '{waste_key}' no encontrada. FU_Total será igual a FU_Calculado.")
                     self.df_params['FU_Total'] = self.df_params['FU_Calculado']

                print("Cálculo de FU_Total para parámetros completado.")

            else:
                QtWidgets.QMessageBox.warning(self, "Advertencia", f"Archivo de parámetros '{os.path.basename(self.params_file)}' no encontrado.")
                # Crear df_params vacío con columna FU_Total para evitar errores posteriores
                self.df_params = pd.DataFrame(columns = [
                    COL_CONSUMIBLE, COL_PN_MANUFACTURABLE, 'FU_Total' # Mínimo necesario para merge
                ])


            # --- Cargar Forecast (FORECAST_BD_PATH) ---
            if os.path.exists(self.forecast_file):
                self.df_forecast = pd.read_csv(self.forecast_file, encoding="utf-8-sig")
                original_cols_f = list(self.df_forecast.columns)
                self.df_forecast.columns = [col.strip().lstrip('ï»¿') for col in original_cols_f]
                print(f"Forecast cargado: {len(self.df_forecast)} filas. Cols limpias: {list(self.df_forecast.columns)}")
            else:
                QtWidgets.QMessageBox.warning(self, "Advertencia", f"Archivo de forecast '{os.path.basename(self.forecast_file)}' no encontrado.")
                self.df_forecast = pd.DataFrame() # Asegurar que sea un DF vacío

            # --- Cargar Inventario (INVENTORY_DB_PATH) ---
            if os.path.exists(self.inventory_file):
                self.df_inventory = pd.read_csv(self.inventory_file, encoding="utf-8-sig")
                original_cols_i = list(self.df_inventory.columns)
                self.df_inventory.columns = [col.strip().lstrip('ï»¿') for col in original_cols_i]
                print(f"Inventario cargado: {len(self.df_inventory)} filas. Cols limpias: {list(self.df_inventory.columns)}")

                # Identificar y asegurar columnas de inventario (incluyendo nuevas)
                on_hand_key = find_column_name(self.df_inventory.columns, [COL_INV_ON_HAND])
                on_order_key = find_column_name(self.df_inventory.columns, [COL_INV_ON_ORDER])
                fisico_key = find_column_name(self.df_inventory.columns, [COL_INV_FISICO])
                safety_key = find_column_name(self.df_inventory.columns, [COL_INV_SAFETY_STOCK])

                inv_num_cols_keys = [on_hand_key, on_order_key]
                for col_key in inv_num_cols_keys:
                    if col_key in self.df_inventory.columns:
                        self.df_inventory[col_key] = pd.to_numeric(self.df_inventory[col_key].astype(str).str.replace(',','.'), errors='coerce').fillna(0)
                    else:
                        print(f"Advertencia Inv: Columna '{col_key}' no encontrada. Añadida con 0.")
                        self.df_inventory[col_key] = 0.0

                # Añadir/asegurar Físico y Safety Stock
                for col_key, default_col_key in [(fisico_key, on_hand_key), (safety_key, None)]:
                    if col_key not in self.df_inventory.columns:
                        print(f"Advertencia Inv: Columna '{col_key}' no encontrada. Añadiendo.")
                        if default_col_key and default_col_key in self.df_inventory.columns:
                            self.df_inventory[col_key] = self.df_inventory[default_col_key].copy()
                        else:
                            self.df_inventory[col_key] = 0.0
                    else:
                         # Asegurar que sea numérica si existe
                         self.df_inventory[col_key] = pd.to_numeric(self.df_inventory[col_key].astype(str).str.replace(',','.'), errors='coerce').fillna(0)
            else:
                QtWidgets.QMessageBox.warning(self, "Advertencia", f"Archivo de inventario '{os.path.basename(self.inventory_file)}' no encontrado.")
                self.df_inventory = pd.DataFrame() # Asegurar DF vacío

        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error de Carga Inicial", f"Error al cargar archivos base:\n{e}")
            traceback.print_exc()
            # Resetear DataFrames en caso de error
            self.df_params = pd.DataFrame(columns = [COL_CONSUMIBLE, COL_PN_MANUFACTURABLE, 'FU_Total']) # Mínimo
            self.df_forecast = pd.DataFrame()
            self.df_inventory = pd.DataFrame()
        print("Carga inicial de datos para Panel de Demanda completada.")


    # --- calculate_demand (SIN CAMBIOS EN LA LÓGICA CENTRAL) ---
    # Depende del FU_Total correctamente calculado en load_initial_data
    def calculate_demand(self):
        """Realiza el cálculo completo de demanda y estado de inventario."""
        print("Iniciando cálculo de demanda...")
        self.load_initial_data() # Recargar y recalcular FU_Total

        if self.df_params.empty or self.df_forecast.empty:
            # Chequear si df_params tiene al menos las columnas mínimas
            if not all(c in self.df_params.columns for c in [COL_CONSUMIBLE, COL_PN_MANUFACTURABLE, 'FU_Total']):
                 QtWidgets.QMessageBox.information(self, "Faltan Datos", "No se pueden calcular la demanda. Faltan datos o columnas esenciales en Parámetros o Forecast.")
                 return
            elif self.df_forecast.empty:
                 QtWidgets.QMessageBox.information(self, "Faltan Datos", "No se pueden calcular la demanda. Faltan datos de Forecast.")
                 return


        try:
            # --- Preparación (Usa DFs de self) ---
            df_p = self.df_params.copy()
            df_f = self.df_forecast.copy()
            df_i = self.df_inventory.copy()

            # --- Identificar columnas clave ---
            param_pn_key = find_column_name(df_p.columns, [COL_PN_MANUFACTURABLE, "PN_Manufacturable"])
            forecast_pn_key = find_column_name(df_f.columns, [COL_FORECAST_PN, "PN Manufacturable", "PN_Manufacturable"])
            inv_item_key = find_column_name(df_i.columns, [COL_INV_ITEM, "Consumible", "Item Code", "ITEM", "ItemNumber"]) # Añadir alias
            consumible_key = find_column_name(df_p.columns, [COL_CONSUMIBLE])

            # --- Identificar y Sumar Columnas de Forecast ---
            forecast_cols = []
            # Patrón más flexible: 3+ letras opcionalmente sep por '-' o ' ', seguido de 2 o 4 dígitos
            # O solo números si es semana (ej. 2501) - ajustar si es necesario
            month_year_pattern = re.compile(r"^(?:[a-zA-Z]{3,}[\s-]?)?\d{2,4}$")
            numeric_pattern = re.compile(r"^\d+$") # Para columnas puramente numéricas (semanas?)

            potential_qty_cols = []
            for col in df_f.columns:
                 col_str = str(col).strip()
                 # Priorizar patrón Mes-Año
                 if month_year_pattern.match(col_str):
                     potential_qty_cols.append(col)
                 # Considerar columnas numéricas si no coinciden con mes-año
                 # Excluir columnas clave conocidas como PN, Edificio, Proyecto
                 elif numeric_pattern.match(col_str) and col not in [forecast_pn_key, COL_EDIFICIO, COL_PROJECT]:
                      # Podría ser una semana o cantidad directa, añadir heurística si es necesario
                      # Por ahora, la añadimos como potencial columna de cantidad
                      potential_qty_cols.append(col)


            if not potential_qty_cols:
                 # Si no se encuentran columnas con patrón, buscar la columna 'Quantity' explícitamente
                  qty_key = find_column_name(df_f.columns, [COL_FORECAST_QTY, 'Quantity', 'Qty'])
                  if qty_key in df_f.columns:
                      print(f"No se encontraron columnas de forecast por patrón. Usando columna explícita: '{qty_key}'")
                      df_f['Total_Forecast_Qty'] = pd.to_numeric(df_f[qty_key], errors='coerce').fillna(0)
                  else:
                       raise ValueError("No se encontraron columnas de cantidad de forecast (patrón mes-año/numérico o 'Quantity').")
            else:
                 print(f"Columnas de forecast potenciales identificadas por patrón: {potential_qty_cols}")
                 # Convertir todas las potenciales a numérico antes de sumar
                 for col in potential_qty_cols:
                      df_f[col] = pd.to_numeric(df_f[col], errors='coerce').fillna(0)
                 df_f['Total_Forecast_Qty'] = df_f[potential_qty_cols].sum(axis=1)
                 print("Columna 'Total_Forecast_Qty' calculada sumando columnas por patrón.")


            # --- Validar columnas necesarias ---
            required_forecast_cols = [forecast_pn_key, 'Total_Forecast_Qty']
            if not all(col in df_f.columns for col in required_forecast_cols):
                missing = [col for col in required_forecast_cols if col not in df_f.columns]
                raise ValueError(f"Faltan columnas esenciales en Forecast: {missing}")

            required_param_cols = [consumible_key, param_pn_key, 'FU_Total']
            if not all(col in df_p.columns for col in required_param_cols):
                missing = [col for col in required_param_cols if col not in df_p.columns]
                raise ValueError(f"Faltan columnas esenciales en Parámetros: {missing}")

            # --- Asegurar FU_Total es numérico en Parámetros ---
            df_p['FU_Total'] = pd.to_numeric(df_p['FU_Total'], errors='coerce').fillna(0)

            # --- Preparar para Merge 1 ---
            df_p_relevant = df_p[[consumible_key, param_pn_key, 'FU_Total']].dropna(subset=[param_pn_key, consumible_key])
            df_f_to_merge = df_f[[forecast_pn_key, 'Total_Forecast_Qty']].dropna(subset=[forecast_pn_key])
            df_f_to_merge[forecast_pn_key] = df_f_to_merge[forecast_pn_key].astype(str).str.strip()
            df_p_relevant[param_pn_key] = df_p_relevant[param_pn_key].astype(str).str.strip()

            # --- Merge 1: Forecast con Parámetros ---
            print(f"Merge 1: Forecast ({len(df_f_to_merge)}) vs Parámetros ({len(df_p_relevant)}) on PN")
            df_merged1 = pd.merge(
                df_f_to_merge,
                df_p_relevant,
                left_on=forecast_pn_key,
                right_on=param_pn_key,
                how='left' # Mantener todas las filas de forecast
            )
            print(f"Resultado Merge 1: {len(df_merged1)} filas.")

            # Manejar PNs sin parámetro FU encontrado
            missing_fu_mask = df_merged1[consumible_key].isna()
            if missing_fu_mask.any():
                missing_pns = df_merged1.loc[missing_fu_mask, forecast_pn_key].unique()
                print(f"Advertencia: {len(missing_pns)} PNs del forecast no encontraron parámetros FU.")
                QtWidgets.QMessageBox.warning(self, "Parámetros Faltantes",
                                              f"No se encontraron parámetros de FU para {len(missing_pns)} PNs.\n"
                                              f"La demanda para estos PNs será 0.")
                # Llenar FU_Total con 0 y crear un Consumible placeholder
                df_merged1.loc[missing_fu_mask, 'FU_Total'] = 0
                # Evitar error si consumible_key no estaba originalmente en df_merged1 (aunque merge debería añadirlo)
                if consumible_key not in df_merged1.columns: df_merged1[consumible_key] = pd.NA
                df_merged1.loc[missing_fu_mask, consumible_key] = "SIN_PARAMETRO_" + df_merged1.loc[missing_fu_mask, forecast_pn_key].astype(str)


            # --- Calcular Demanda por Línea (usa FU_Total) ---
            df_merged1['Demand_PN'] = df_merged1['Total_Forecast_Qty'] * df_merged1['FU_Total']
            print("Demanda por línea de forecast calculada ('Demand_PN').")

            # --- Agregar Demanda por Consumible ---
            df_merged1[consumible_key] = df_merged1[consumible_key].astype(str) # Asegurar string
            df_demand_agg = df_merged1.groupby(consumible_key, dropna=False)['Demand_PN'].sum().reset_index() # No dropear NA aquí
            df_demand_agg.rename(columns={'Demand_PN': 'Demanda_Total'}, inplace=True)
            # Filtrar filas donde el consumible sea placeholder (porque no tenían parámetro)
            df_demand_agg = df_demand_agg[~df_demand_agg[consumible_key].str.startswith("SIN_PARAMETRO_", na=True)]
            print(f"Demanda agregada por consumible: {len(df_demand_agg)} consumibles válidos.")


            # --- Merge 2: Demanda Agregada con Inventario ---
            if df_i.empty:
                print("Archivo de inventario vacío. Creando tabla sin datos de inventario.")
                self.df_demand = df_demand_agg.copy()
                 # Identificar nombres estándar para columnas de inventario a añadir
                on_hand_key_std = find_column_name([], [COL_INV_ON_HAND]) # Usa solo el nombre estándar
                on_order_key_std = find_column_name([], [COL_INV_ON_ORDER])
                fisico_key_std = find_column_name([], [COL_INV_FISICO])
                safety_key_std = find_column_name([], [COL_INV_SAFETY_STOCK])
                for col_key_std in [on_hand_key_std, on_order_key_std, fisico_key_std, safety_key_std]:
                    self.df_demand[col_key_std] = 0.0
            else:
                print(f"Merge 2: Demanda Agregada ({len(df_demand_agg)}) vs Inventario ({len(df_i)}) on Consumible/Item")
                # Identificar claves de inventario (ya limpias y aseguradas en load_initial_data)
                on_hand_key = find_column_name(df_i.columns, [COL_INV_ON_HAND])
                on_order_key = find_column_name(df_i.columns, [COL_INV_ON_ORDER])
                fisico_key = find_column_name(df_i.columns, [COL_INV_FISICO])
                safety_key = find_column_name(df_i.columns, [COL_INV_SAFETY_STOCK])

                # Asegurar que inv_item_key existe
                if inv_item_key not in df_i.columns:
                    raise ValueError(f"La columna clave '{inv_item_key}' no se encontró en el archivo de inventario después de la carga.")

                # Convertir claves a string
                df_demand_agg[consumible_key] = df_demand_agg[consumible_key].astype(str).str.strip()
                df_i[inv_item_key] = df_i[inv_item_key].astype(str).str.strip()

                # Seleccionar columnas de inventario
                inv_cols_to_merge = [inv_item_key] + [col for col in [on_hand_key, on_order_key, fisico_key, safety_key] if col in df_i.columns]
                df_i_relevant = df_i[inv_cols_to_merge].drop_duplicates(subset=[inv_item_key], keep='first') # Evitar duplicados en inventario

                # Realizar merge
                self.df_demand = pd.merge(
                    df_demand_agg,
                    df_i_relevant,
                    left_on=consumible_key,
                    right_on=inv_item_key,
                    how='left' # Mantener todos los consumibles con demanda
                )
                print(f"Resultado Merge 2: {len(self.df_demand)} filas.")

                # Llenar NaNs para inventario no encontrado y asegurar columnas
                inv_cols_to_fill_keys = [on_hand_key, on_order_key, fisico_key, safety_key]
                for col_key in inv_cols_to_fill_keys:
                    if col_key in self.df_demand.columns:
                        self.df_demand[col_key].fillna(0, inplace=True)
                    else:
                        print(f"Advertencia: Columna '{col_key}' no presente después del merge. Añadida con 0.")
                        self.df_demand[col_key] = 0.0

                # Limpiar claves si son diferentes
                if consumible_key != inv_item_key and inv_item_key in self.df_demand.columns:
                    self.df_demand.drop(columns=[inv_item_key], inplace=True)

            # --- Calcular Necesidad de Pedido ---
            print("Calculando Necesidad de Pedido...")
            # Usar nombres estándar post-merge o los identificados si no hubo merge
            on_hand_final = find_column_name(self.df_demand.columns, [COL_INV_ON_HAND])
            on_order_final = find_column_name(self.df_demand.columns, [COL_INV_ON_ORDER])
            safety_final = find_column_name(self.df_demand.columns, [COL_INV_SAFETY_STOCK])
            fisico_final = find_column_name(self.df_demand.columns, [COL_INV_FISICO]) # Asegurar que existe

            calc_cols_keys = ['Demanda_Total', on_hand_final, on_order_final, safety_final]
            for col_key in calc_cols_keys:
                 if col_key in self.df_demand.columns:
                     self.df_demand[col_key] = pd.to_numeric(self.df_demand[col_key], errors='coerce').fillna(0)
                 else:
                     print(f"Advertencia Calc: Columna '{col_key}' no encontrada. Añadida con 0.")
                     self.df_demand[col_key] = 0.0

            # Asegurar que fisico_final también sea numérico
            if fisico_final in self.df_demand.columns:
                 self.df_demand[fisico_final] = pd.to_numeric(self.df_demand[fisico_final], errors='coerce').fillna(0)
            else:
                 print(f"Advertencia Calc: Columna '{fisico_final}' no encontrada. Añadida con 0.")
                 self.df_demand[fisico_final] = 0.0


            # Cálculo de Necesidad
            inventario_proyectado = self.df_demand[on_hand_final] + self.df_demand[on_order_final]
            self.df_demand['Necesidad_Pedido'] = (
                self.df_demand['Demanda_Total'] -
               (inventario_proyectado - self.df_demand[safety_final])
            )
            self.df_demand['Necesidad_Pedido'] = self.df_demand['Necesidad_Pedido'].clip(lower=0)
            print("Cálculo de Necesidad de Pedido completado.")

            # --- Renombrar columnas finales a estándar para mostrar ---
            rename_map = {
                 consumible_key: COL_CONSUMIBLE, # Asegurar que la clave final sea COL_CONSUMIBLE
                 on_hand_final: COL_INV_ON_HAND,
                 on_order_final: COL_INV_ON_ORDER,
                 fisico_final: COL_INV_FISICO,
                 safety_final: COL_INV_SAFETY_STOCK
            }
            rename_map_filtered = {k: v for k, v in rename_map.items() if k in self.df_demand.columns}
            self.df_demand.rename(columns=rename_map_filtered, inplace=True)
            print(f"Columnas renombradas a estándar final: {list(self.df_demand.columns)}")

            # --- Mostrar Resultados ---
            self.display_demand_data()
            self.btn_save_inventory.setEnabled(False)
            print("Cálculo de demanda finalizado y tabla actualizada.")

        except KeyError as e:
            QtWidgets.QMessageBox.critical(self, "Error de Columna", f"No se encontró la columna esperada: {e}\nVerifica nombres en CSVs.")
            traceback.print_exc()
        except ValueError as e:
            QtWidgets.QMessageBox.critical(self, "Error de Valor o Configuración", f"Error en datos o config: {e}")
            traceback.print_exc()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error de Cálculo", f"Error inesperado al calcular demanda:\n{e}")
            traceback.print_exc()
            self.df_demand = pd.DataFrame()
            self.display_demand_data()

    # --- display_demand_data (SIN CAMBIOS) ---
    # Muestra df_demand que ahora tiene los cálculos correctos
    def display_demand_data(self):
        """Muestra el DataFrame de demanda/inventario en la tabla."""
        self.table.clearContents()
        self.table.setRowCount(0)

        if self.df_demand.empty:
            print("No hay datos de demanda para mostrar.")
            # Opcional: Limpiar headers si la tabla estaba poblada antes
            # self.table.setColumnCount(0)
            return

        # Ordenar columnas para mostrar consistentemente
        display_columns_ordered = [
             COL_CONSUMIBLE, 'Demanda_Total',
             COL_INV_ON_HAND, COL_INV_ON_ORDER, COL_INV_FISICO,
             COL_INV_SAFETY_STOCK, 'Necesidad_Pedido'
        ]
        # Incluir solo las columnas que existen en el df_demand final
        display_columns = [col for col in display_columns_ordered if col in self.df_demand.columns]
        # Añadir cualquier otra columna que pudiera existir en df_demand al final
        other_cols = [col for col in self.df_demand.columns if col not in display_columns]
        display_columns.extend(other_cols)


        # Configurar columnas si es necesario
        if self.table.columnCount() != len(display_columns):
            self.table.setColumnCount(len(display_columns))
            self.table.setHorizontalHeaderLabels(display_columns)

        self.table.setRowCount(len(self.df_demand))

        # --- Configurar Editabilidad y Delegados ---
        numeric_delegate = NumericDelegate(self.table)
        editable_cols_indices = []
        fisico_col_idx = -1
        on_hand_col_idx = -1
        necesidad_col_idx = -1

        # Identificar índices (asegurarse que las columnas existan)
        try: fisico_col_idx = display_columns.index(COL_INV_FISICO)
        except ValueError: fisico_col_idx = -1
        try: on_hand_col_idx = display_columns.index(COL_INV_ON_HAND)
        except ValueError: on_hand_col_idx = -1
        try: necesidad_col_idx = display_columns.index('Necesidad_Pedido')
        except ValueError: necesidad_col_idx = -1
        try: safety_col_idx = display_columns.index(COL_INV_SAFETY_STOCK)
        except ValueError: safety_col_idx = -1


        # Aplicar delegado a columnas editables
        editable_standard_names = [COL_INV_FISICO, COL_INV_SAFETY_STOCK]
        for j, col_name in enumerate(display_columns):
             if col_name in editable_standard_names:
                 self.table.setItemDelegateForColumn(j, numeric_delegate)
                 editable_cols_indices.append(j)

        # --- Llenar Tabla y Aplicar Formato Condicional ---
        # Usar el df_demand directamente (ya renombrado y calculado)
        df_display_final = self.df_demand.copy()

        # Bloquear señales mientras se llena la tabla masivamente
        self.table.blockSignals(True)
        try:
            for i, (idx, row) in enumerate(df_display_final.iterrows()):
                # Usar el índice del DF como referencia si es necesario, aunque no se muestra
                # header_item = QtWidgets.QTableWidgetItem(str(idx))
                # self.table.setVerticalHeaderItem(i, header_item)

                for j, col_name in enumerate(display_columns):
                    item_value = row[col_name]

                    # Formatear números
                    try_float = None
                    if isinstance(item_value, (int, float, np.number)):
                        try_float = float(item_value)
                    elif isinstance(item_value, str):
                         try_float = safe_float_conversion(item_value, None)

                    if try_float is not None and not pd.isna(try_float):
                        item_text = f"{try_float:.5f}" # 5 decimales para FU/Inventario
                        alignment = QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter
                    else:
                        item_text = str(item_value) if pd.notna(item_value) else "" # Mostrar vacío si es NA/NaN
                        alignment = QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter

                    table_item = QtWidgets.QTableWidgetItem(item_text)
                    table_item.setTextAlignment(alignment)

                    # Editabilidad
                    if j in editable_cols_indices:
                         table_item.setFlags(table_item.flags() | QtCore.Qt.ItemIsEditable)
                         # Pasar valor numérico al delegado si es posible, si no, el texto original
                         table_item.setData(QtCore.Qt.EditRole, try_float if try_float is not None else item_value)
                    else:
                         table_item.setFlags(table_item.flags() & ~QtCore.Qt.ItemIsEditable)


                    # --- Formato Condicional ---
                    # Diferencia Físico vs On Hand
                    if fisico_col_idx != -1 and on_hand_col_idx != -1 and j == fisico_col_idx:
                        fisico_val = safe_float_conversion(row.get(COL_INV_FISICO), None) # Usar .get para seguridad
                        on_hand_val = safe_float_conversion(row.get(COL_INV_ON_HAND), None)
                        # Usar una pequeña tolerancia para comparar flotantes
                        if fisico_val is not None and on_hand_val is not None and \
                           not np.isclose(fisico_val, on_hand_val, atol=1e-5): # Tolerancia pequeña
                               table_item.setBackground(QtGui.QColor(255, 255, 180)) # Amarillo pálido
                               table_item.setToolTip(f"Inv Físico ({fisico_val:.3f}) difiere de On Hand ({on_hand_val:.3f})")
                        # else: # No es necesario resetear el color si se usa alternatingRowColors
                        #    table_item.setBackground(QtGui.QColor(QtCore.Qt.white))
                        #    table_item.setToolTip("")

                    # Necesidad de Pedido > 0
                    if necesidad_col_idx != -1 and j == necesidad_col_idx:
                        necesidad_val = safe_float_conversion(row.get('Necesidad_Pedido'), 0.0)
                        if necesidad_val > 1e-5: # Usar tolerancia > 0
                            table_item.setBackground(QtGui.QColor(255, 180, 180)) # Rojo pálido
                            table_item.setForeground(QtGui.QColor(150, 0, 0)) # Texto oscuro
                            table_item.setToolTip(f"Se necesita pedir {necesidad_val:.3f} unidades.")
                        # else:
                        #    table_item.setBackground(QtGui.QColor(QtCore.Qt.white))
                        #    table_item.setForeground(QtGui.QColor(QtCore.Qt.black))
                        #    table_item.setToolTip("")

                    self.table.setItem(i, j, table_item)
        finally:
             self.table.blockSignals(False) # Reactivar señales

        self.table.resizeColumnsToContents()
        if self.table.columnCount() > 0:
            try:
                 self.table.horizontalHeader().setStretchLastSection(True)
            except Exception as e:
                 print(f"Error ajustando última sección (Demanda): {e}")
        print(f"Tabla de demanda poblada con {self.table.rowCount()} filas.")


    # --- on_item_changed (SIN CAMBIOS FUNCIONALES MAYORES) ---
    # Ya recalcula Necesidad_Pedido si cambia Safety Stock
    # Ya aplica formato condicional para Inv Fisico
    def on_item_changed(self, item):
        """Se activa cuando un item editable de la tabla cambia."""
        col = item.column()
        row_index = item.row() # Índice de la fila en la VISTA de la tabla
        headers = [self.table.horizontalHeaderItem(j).text() for j in range(self.table.columnCount())]
        col_name = headers[col] # Nombre estándar de la columna

        # Actuar solo si es una columna editable
        if col_name in [COL_INV_FISICO, COL_INV_SAFETY_STOCK]:
            print(f"Cambio detectado: Fila {row_index}, Col '{col_name}', Nuevo texto: '{item.text()}'")
            self.btn_save_inventory.setEnabled(True) # Habilitar guardado

            # --- Actualizar DataFrame interno df_demand ---
            try:
                # Encontrar el índice CORRESPONDIENTE en el DataFrame df_demand
                # Esto es crucial si la tabla está filtrada o sorteada de forma diferente al DF
                # La forma más segura es buscar por una clave única, como COL_CONSUMIBLE
                consumible_col_idx = -1
                try: consumible_col_idx = headers.index(COL_CONSUMIBLE)
                except ValueError: pass

                if consumible_col_idx == -1:
                     print("Error: No se encuentra la columna 'Consumible' en la tabla para identificar la fila del DataFrame.")
                     return

                consumible_item = self.table.item(row_index, consumible_col_idx)
                if not consumible_item:
                     print(f"Error: No se pudo obtener el item 'Consumible' para la fila {row_index}.")
                     return
                consumible_val = consumible_item.text()

                # Buscar el índice en df_demand que coincide con este consumible
                # Esto asume que COL_CONSUMIBLE es único en df_demand (lo cual debería ser después del groupby)
                matching_indices = self.df_demand.index[self.df_demand[COL_CONSUMIBLE] == consumible_val].tolist()

                if len(matching_indices) != 1:
                     print(f"Error: Se encontraron {len(matching_indices)} índices en df_demand para Consumible '{consumible_val}'. No se puede actualizar.")
                     # Podría haber problemas con espacios extra o casos si la comparación falla
                     # print(self.df_demand[COL_CONSUMIBLE].unique()) # Debug: Ver valores únicos
                     return
                df_index = matching_indices[0] # Índice correcto en el DataFrame

                # Obtener el nuevo valor numérico
                new_value = safe_float_conversion(item.text(), None)
                if new_value is None:
                    print(f"Error: Valor inválido '{item.text()}' ingresado. No se actualizará el DataFrame.")
                    # Podríamos revertir el texto en la celda o mostrar error
                    # item.setText(str(self.df_demand.loc[df_index, col_name])) # Revertir (requiere bloquear señales)
                    return

                # Actualizar df_demand
                self.df_demand.loc[df_index, col_name] = new_value
                print(f"DataFrame df_demand actualizado en índice {df_index}, columna '{col_name}' con valor {new_value}")

                # --- Recalcular Necesidad y actualizar formato/celdas dependientes ---
                self.table.blockSignals(True)
                try:
                    row_data = self.df_demand.loc[df_index] # Obtener fila actualizada del DF

                    # Si cambió Safety Stock, recalcular Necesidad Pedido
                    if col_name == COL_INV_SAFETY_STOCK:
                         # Usar nombres estándar que sabemos existen en df_demand
                         inventario_proyectado = row_data.get(COL_INV_ON_HAND, 0) + row_data.get(COL_INV_ON_ORDER, 0)
                         demanda_total = row_data.get('Demanda_Total', 0)
                         safety_stock = row_data.get(COL_INV_SAFETY_STOCK, 0) # Usa el nuevo valor

                         necesidad = (demanda_total - (inventario_proyectado - safety_stock))
                         necesidad = max(0, necesidad) # No puede ser negativo
                         self.df_demand.loc[df_index, 'Necesidad_Pedido'] = necesidad
                         print(f"Necesidad recalculada para índice {df_index}: {necesidad}")

                         # Actualizar celda de necesidad en la tabla
                         necesidad_col_idx = -1
                         try: necesidad_col_idx = headers.index('Necesidad_Pedido')
                         except ValueError: pass

                         if necesidad_col_idx != -1:
                             necesidad_item = self.table.item(row_index, necesidad_col_idx)
                             if necesidad_item:
                                 necesidad_item.setText(f"{necesidad:.5f}") # Actualizar texto
                                 # Reaplicar formato condicional para Necesidad
                                 if necesidad > 1e-5:
                                     necesidad_item.setBackground(QtGui.QColor(255, 180, 180))
                                     necesidad_item.setForeground(QtGui.QColor(150, 0, 0))
                                     necesidad_item.setToolTip(f"Se necesita pedir {necesidad:.3f} unidades.")
                                 else:
                                     # Resetear color (importante si se usa alternating)
                                     necesidad_item.setBackground(QtGui.QColor(QtCore.Qt.white)) # O color base
                                     necesidad_item.setForeground(QtGui.QColor(QtCore.Qt.black))
                                     necesidad_item.setToolTip("")

                    # Si cambió Inventario Físico, actualizar su formato condicional
                    if col_name == COL_INV_FISICO:
                        fisico_col_idx = col # Ya tenemos el índice de la columna cambiada
                        on_hand_col_idx = -1
                        try: on_hand_col_idx = headers.index(COL_INV_ON_HAND)
                        except ValueError: pass

                        if on_hand_col_idx != -1:
                             on_hand_item = self.table.item(row_index, on_hand_col_idx)
                             if on_hand_item:
                                 on_hand_val = safe_float_conversion(on_hand_item.text(), None)
                                 fisico_val = new_value # El valor que se acaba de ingresar

                                 # Reaplicar formato condicional para Inv Fisico
                                 if fisico_val is not None and on_hand_val is not None and \
                                    not np.isclose(fisico_val, on_hand_val, atol=1e-5):
                                         item.setBackground(QtGui.QColor(255, 255, 180))
                                         item.setToolTip(f"Inv Físico ({fisico_val:.3f}) difiere de On Hand ({on_hand_val:.3f})")
                                 else:
                                     item.setBackground(QtGui.QColor(QtCore.Qt.white)) # O color base
                                     item.setToolTip("")
                finally:
                    self.table.blockSignals(False) # Desbloquear señales

            except KeyError as e:
                 print(f"Error de clave al actualizar df_demand o recalcular: {e}")
                 # Podría ocurrir si falta una columna esperada en row_data (ej. 'Demanda_Total')
            except Exception as e:
                 print(f"Error inesperado en on_item_changed: {e}")
                 traceback.print_exc()


    # --- save_inventory_changes (SIN CAMBIOS FUNCIONALES) ---
    # Guarda las columnas COL_INV_FISICO y COL_INV_SAFETY_STOCK del df_demand
    # al archivo original de inventario (SKID_TOOLTRACK+.csv)
    def save_inventory_changes(self):
        """Guarda las columnas editables (Inv. Físico, Safety Stock) de df_demand de vuelta al archivo CSV de inventario."""
        if self.df_demand.empty or not self.btn_save_inventory.isEnabled():
            print("No hay cambios pendientes o datos de demanda para guardar en inventario.")
            return

        if not os.path.exists(self.inventory_file):
            QtWidgets.QMessageBox.critical(self, "Error", f"No se puede guardar. El archivo de inventario '{os.path.basename(self.inventory_file)}' no existe.")
            return

        print(f"Intentando guardar cambios de inventario en {os.path.basename(self.inventory_file)}...")
        lock_path = self.inventory_file + ".lock"
        lock = FileLock(lock_path, timeout=10) # Espera hasta 10 segundos

        try:
            with lock:
                # Leer el archivo de inventario original para preservar otras columnas/filas
                df_inv_original = pd.read_csv(self.inventory_file, encoding="utf-8-sig")
                original_cols_i = list(df_inv_original.columns) # Guardar columnas originales con BOM/espacios
                # Limpiar columnas del original para trabajar
                df_inv_original.columns = [col.strip().lstrip('ï»¿') for col in original_cols_i]
                print(f"Archivo inventario original leído ({len(df_inv_original)} filas). Cols limpias: {list(df_inv_original.columns)}")

                # Identificar la columna clave ITEM en el archivo original (limpia)
                inv_item_key = find_column_name(df_inv_original.columns, [COL_INV_ITEM, "Consumible", "Item Code", "ITEM", "ItemNumber"])
                if inv_item_key not in df_inv_original.columns:
                     raise KeyError(f"No se encontró la columna clave '{inv_item_key}' en el archivo de inventario {os.path.basename(self.inventory_file)}")
                print(f"Columna clave identificada en inventario: '{inv_item_key}'")

                # Preparar los datos actualizados desde df_demand (que usa nombres estándar)
                cols_to_update_from_demand = [COL_CONSUMIBLE] # Clave para merge
                if COL_INV_FISICO in self.df_demand.columns: cols_to_update_from_demand.append(COL_INV_FISICO)
                if COL_INV_SAFETY_STOCK in self.df_demand.columns: cols_to_update_from_demand.append(COL_INV_SAFETY_STOCK)

                if len(cols_to_update_from_demand) <= 1:
                     print("No hay columnas editables (Inv Fisico, Safety Stock) en los datos de demanda para guardar.")
                     return

                df_updates = self.df_demand[cols_to_update_from_demand].copy()
                print(f"Datos a actualizar desde df_demand ({len(df_updates)} filas): {cols_to_update_from_demand}")

                # Convertir claves a string para merge/update
                df_inv_original[inv_item_key] = df_inv_original[inv_item_key].astype(str).str.strip()
                df_updates[COL_CONSUMIBLE] = df_updates[COL_CONSUMIBLE].astype(str).str.strip()

                # --- Estrategia: Actualizar usando el índice ---
                # Renombrar columnas en df_updates para que coincidan con las del inventario original
                # Necesitamos los nombres limpios correspondientes en df_inv_original
                fisico_key_orig = find_column_name(df_inv_original.columns, [COL_INV_FISICO])
                safety_key_orig = find_column_name(df_inv_original.columns, [COL_INV_SAFETY_STOCK])

                rename_map_save = {COL_CONSUMIBLE: inv_item_key} # Mapear clave
                if COL_INV_FISICO in df_updates.columns: rename_map_save[COL_INV_FISICO] = fisico_key_orig
                if COL_INV_SAFETY_STOCK in df_updates.columns: rename_map_save[COL_INV_SAFETY_STOCK] = safety_key_orig

                df_updates.rename(columns=rename_map_save, inplace=True)
                print(f"Columnas renombradas en df_updates para guardar: {list(df_updates.columns)}")

                # Crear índice en ambos DFs basado en la clave de inventario
                df_inv_original_indexed = df_inv_original.set_index(inv_item_key, drop=False) # drop=False para mantener la columna
                df_updates_indexed = df_updates.set_index(inv_item_key) # Clave ya renombrada a inv_item_key

                # Actualizar usando update() - modifica df_inv_original_indexed inplace
                df_inv_original_indexed.update(df_updates_indexed)
                print("DataFrame original actualizado en memoria.")

                # Volver a DataFrame normal
                df_inv_updated = df_inv_original_indexed.reset_index(drop=True)

                # --- Reordenar y Renombrar Columnas al Original ---
                final_columns_clean = [col.strip().lstrip('ï»¿') for col in original_cols_i] # Nombres limpios en orden original

                # Asegurar que todas las columnas limpias originales existan en el actualizado
                for col_clean in final_columns_clean:
                    if col_clean not in df_inv_updated.columns:
                        print(f"Advertencia Save: Columna original '{col_clean}' no encontrada en df actualizado. Añadiendo como NA.")
                        df_inv_updated[col_clean] = pd.NA

                # Seleccionar y reordenar usando nombres limpios
                df_inv_to_save = df_inv_updated[final_columns_clean]
                # Renombrar de vuelta a los nombres originales (con BOM/espacios)
                df_inv_to_save.columns = original_cols_i

                # --- Guardar ---
                df_inv_to_save.to_csv(self.inventory_file, index=False, encoding="utf-8-sig", float_format='%.5f') # Mantener BOM y precisión
                print("Archivo de inventario guardado exitosamente.")

                QtWidgets.QMessageBox.information(self, "Éxito", f"Cambios de inventario guardados en\n{os.path.basename(self.inventory_file)}")
                self.btn_save_inventory.setEnabled(False) # Deshabilitar botón

        except FileLock.Timeout:
            print(f"Error: Timeout al intentar bloquear '{self.inventory_file}'.")
            QtWidgets.QMessageBox.warning(self, "Error de Bloqueo", f"No se pudo acceder al archivo de inventario:\n{os.path.basename(self.inventory_file)}\n\nOtro proceso podría estar usándolo. Intenta de nuevo.")
        except KeyError as e:
             QtWidgets.QMessageBox.critical(self, "Error de Columna al Guardar", f"No se encontró una columna clave esperada: {e}")
             traceback.print_exc()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error al Guardar Inventario", f"No se pudieron guardar los cambios:\n{e}")
            traceback.print_exc()


# --- Contenedor Principal (sin cambios) ---
class Window3Page(QtWidgets.QWidget):
    """ Contenedor principal para las pestañas. """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Módulo - Factor de Uso y Demanda")
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self.tabs = QtWidgets.QTabWidget()

        # Crear instancias de las pestañas
        try:
            self.tab_factor = TabFactorUso(FU_DB_PATH, parent=self.tabs)
            self.tabs.addTab(self.tab_factor, "Factor de Uso (Parámetros)")
        except Exception as e:
            print(f"Error al crear TabFactorUso: {e}")
            self.tabs.addTab(QtWidgets.QLabel(f"Error TabFactorUso:\n{e}"), "Factor Uso [Error]")

        try:
            self.tab_forecast = TabForecast(FORECAST_BD_PATH, parent=self.tabs)
            self.tabs.addTab(self.tab_forecast, "Forecast (Datos)")
        except Exception as e:
            print(f"Error al crear TabForecast: {e}")
            self.tabs.addTab(QtWidgets.QLabel(f"Error TabForecast:\n{e}"), "Forecast [Error]")

        try:
            self.tab_demanda = TabDemanda(FU_DB_PATH, FORECAST_BD_PATH, INVENTORY_DB_PATH, parent=self.tabs)
            self.tabs.addTab(self.tab_demanda, "Panel de Demanda e Inventario")
        except Exception as e:
            print(f"Error al crear TabDemanda: {e}")
            traceback.print_exc()
            self.tabs.addTab(QtWidgets.QLabel(f"Error TabDemanda:\n{e}"), "Panel Demanda [Error]")

        layout.addWidget(self.tabs)


# ================================================================
# Diálogo para editar mantenimiento (Con Estilos Aplicados)
# ================================================================
class EditMaintenanceDialog(QtWidgets.QDialog):
    def __init__(self, current_item, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Editar Datos de Mantenimiento")
        self.setMinimumWidth(400) # Ajustar ancho mínimo
        # Aplicar estilos base al diálogo
        self.setStyleSheet(GENERAL_STYLESHEET)

        layout = QtWidgets.QFormLayout(self)
        layout.setSpacing(10) # Espacio entre filas

        # --- Campo Último Mantenimiento ---
        self.ultimo_edit = QtWidgets.QLineEdit()
        self.ultimo_edit.setPlaceholderText("dd/mm/yyyy")
        # Permitir formato dd/mm/yyyy o N/A (insensible a mayúsculas)
        regex = QtCore.QRegExp(r"^((\d{2}/\d{2}/\d{4})|([nN]/[aA]))$")
        validator = QtGui.QRegExpValidator(regex, self)
        self.ultimo_edit.setValidator(validator)
        ultimo_value = current_item.get("ULTIMO_MANTENIMIENTO", "")
        # Mostrar valor existente o vacío si es N/A o nulo
        if pd.notna(ultimo_value) and str(ultimo_value).strip().upper() != "N/A":
            # Intentar formatear por si acaso, aunque debería venir bien
            try:
                 current_date = datetime.strptime(str(ultimo_value), "%d/%m/%Y").date()
                 self.ultimo_edit.setText(current_date.strftime("%d/%m/%Y"))
            except ValueError:
                 self.ultimo_edit.setText("") # Dejar vacío si el formato es incorrecto
                 self.ultimo_edit.setPlaceholderText(f"Formato inválido: {ultimo_value}")
        layout.addRow("Último Mantenimiento:", self.ultimo_edit)

        # --- Campo Período ---
        self.periodo_combo = QtWidgets.QComboBox()
        periodos_validos = ["Mensual", "Bimestral", "Trimestral", "Semestral", "Anual"] # Usar Mayúsculas iniciales
        self.periodo_combo.addItems(periodos_validos)
        periodo_value = current_item.get("PERIODO", "Mensual") # Default a Mensual
        # Buscar el periodo en la lista de forma insensible a mayúsculas/minúsculas
        found_period = False
        for p in periodos_validos:
             if str(periodo_value).strip().lower() == p.lower():
                  self.periodo_combo.setCurrentText(p)
                  found_period = True
                  break
        if not found_period: # Si no se encontró, seleccionar el default
             self.periodo_combo.setCurrentText("Mensual")
        layout.addRow("Período:", self.periodo_combo)

        # --- Campo Días Alerta ---
        self.dias_edit = QtWidgets.QLineEdit()
        self.dias_edit.setPlaceholderText("Ej: 7")
        dias_value = current_item.get("DIAS_ALERTA", "0") # Default a "0"
        # Mostrar solo si es un número válido, limpiar NaN/inf
        try:
             dias_display = int(float(dias_value)) if pd.notna(dias_value) else 0
             self.dias_edit.setText(str(dias_display))
        except (ValueError, TypeError):
             self.dias_edit.setText("0") # Poner 0 si no es número válido
        # Validar que solo se puedan ingresar números enteros positivos
        self.dias_edit.setValidator(QtGui.QIntValidator(0, 999, self)) # Rango razonable
        layout.addRow("Días de Alerta (antes):", self.dias_edit)

        # --- Botones OK/Cancel con Estilos ---
        self.button_box = QtWidgets.QDialogButtonBox()
        btn_ok = self.button_box.addButton(QtWidgets.QDialogButtonBox.Ok)
        btn_cancel = self.button_box.addButton(QtWidgets.QDialogButtonBox.Cancel)
        btn_ok.setText("Guardar") # Texto en español
        btn_cancel.setText("Cancelar")

        # Aplicar estilos verde/rojo
        btn_ok.setStyleSheet(BTN_STYLE_ACCEPT)
        btn_cancel.setStyleSheet(BTN_STYLE_REJECT)

        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)

    def get_data(self):
        """Obtiene los datos del diálogo, validando la fecha."""
        ultimo = self.ultimo_edit.text().strip()
        # Validar formato de fecha al obtener los datos
        if ultimo.upper() != "N/A":
             try:
                  # Intenta parsear para asegurar formato correcto antes de devolver
                  datetime.strptime(ultimo, "%d/%m/%Y")
             except ValueError:
                  # Si el formato es incorrecto, mostrar error y quizás no cerrar
                  QtWidgets.QMessageBox.warning(self, "Formato Incorrecto",
                                                "El formato de 'Último Mantenimiento' debe ser dd/mm/yyyy o N/A.")
                  # Podríamos evitar que se cierre el diálogo devolviendo None o lanzando excepción
                  # Por ahora, devolveremos vacío para indicar error, o N/A si estaba vacío.
                  ultimo = "N/A" if not ultimo else "" # "" indica formato inválido

        dias_alerta_str = self.dias_edit.text().strip()

        return {
            "ULTIMO_MANTENIMIENTO": ultimo, # Puede ser fecha, "N/A" o "" (inválido)
            "PERIODO": self.periodo_combo.currentText(), # Ya validado al cargar
            "DIAS_ALERTA": dias_alerta_str if dias_alerta_str else "0" # Devolver string, validar al usar
        }

    # Sobreescribir accept para validar antes de cerrar
    def accept(self):
        data = self.get_data()
        if data["ULTIMO_MANTENIMIENTO"] == "": # Si get_data devolvió "" por formato inválido
             # No cerrar el diálogo, el usuario debe corregir
             return
        # Si la validación pasa (no es ""), llamar al accept original
        super().accept()


# ==============================================================================
# Widget de tooltip no interactivo (Sin cambios visuales necesarios aquí)
# ==============================================================================
class NonInteractiveTooltip(QtWidgets.QLabel):
    def __init__(self, text, parent=None):
        super().__init__(text, parent,
                         flags=QtCore.Qt.ToolTip | 
                               QtCore.Qt.FramelessWindowHint | 
                               QtCore.Qt.WindowStaysOnTopHint)
        self.setAttribute(QtCore.Qt.WA_TranslucentBackground)  # Fondo transparente
        self.setAttribute(QtCore.Qt.WA_TransparentForMouseEvents)
        # Establecemos un estilo por defecto (se sobrescribe dinámicamente)
        self.setStyleSheet("""
            QLabel {
                background-color: rgba(50, 50, 50, 0.85);
                color: white;
                border: 1px solid #444;
                border-radius: 4px;
                padding: 5px 8px;
                font-size: 10pt;
            }
        """)
        self.adjustSize()


# ==============================================================================
# Botón personalizado con ayuda (tooltip) (Usando el nuevo tooltip)
# ==============================================================================
class HoverHelpButton(QtWidgets.QPushButton):
    def __init__(self, text, help_text, parent=None):
        super().__init__(text, parent)
        self.help_text = help_text
        self.timer = QtCore.QTimer(self)
        self.timer.setSingleShot(True)
        self.timer.setInterval(1500)  # Intervalo reducido a 1.5 segundos
        self.timer.timeout.connect(self.showHelpTooltip)
        self.custom_tooltip = None

    def enterEvent(self, event):
        if self.help_text:
            self.timer.start()
        super().enterEvent(event)

    def leaveEvent(self, event):
        self.timer.stop()
        if self.custom_tooltip and self.custom_tooltip.isVisible():
            self.custom_tooltip.hide()
        super().leaveEvent(event)

    def showHelpTooltip(self):
        if not self.help_text:
            return

        if self.custom_tooltip is None:
            # Se utiliza el widget raíz como padre para que el tooltip flote sobre todo
            top_level_widget = self.window()
            self.custom_tooltip = NonInteractiveTooltip(self.help_text, top_level_widget)
        else:
            self.custom_tooltip.setText(self.help_text)

        # Extraer el valor de "background-color" desde la hoja de estilos del botón
        stylesheet = self.styleSheet() or ""
        match = re.search(r"background-color\s*:\s*([^;]+);", stylesheet, re.IGNORECASE)
        if match:
            bg_color = match.group(1).strip()
        else:
            bg_color = "rgba(50, 50, 50, 0.85)"  # Valor por defecto

        # Actualizar la hoja de estilos del tooltip para reflejar el color extraído
        self.custom_tooltip.setStyleSheet(f"""
            QLabel {{
                background-color: {bg_color};
                color: white;
                border: 1px solid #444;
                border-radius: 4px;
                padding: 5px 8px;
                font-size: 10pt;
            }}
        """)
        self.custom_tooltip.adjustSize()

        # Calcular la posición global relativa al botón y aplicar un offset
        global_pos = self.mapToGlobal(self.rect().bottomLeft())
        global_pos.setX(global_pos.x() + 5)
        global_pos.setY(global_pos.y() + 5)

        # Ajuste de posición para evitar que el tooltip se salga de la pantalla
        screen_rect = QtWidgets.QApplication.screenAt(global_pos).availableGeometry()
        tooltip_rect = self.custom_tooltip.frameGeometry()
        tooltip_rect.moveTopLeft(global_pos)
        if tooltip_rect.right() > screen_rect.right():
            global_pos.setX(screen_rect.right() - tooltip_rect.width())
        if tooltip_rect.left() < screen_rect.left():
            global_pos.setX(screen_rect.left())
        if tooltip_rect.bottom() > screen_rect.bottom():
            top_pos = self.mapToGlobal(self.rect().topLeft())
            global_pos.setY(top_pos.y() - tooltip_rect.height() - 5)
            tooltip_rect.moveTopLeft(global_pos)
            if tooltip_rect.right() > screen_rect.right():
                global_pos.setX(screen_rect.right() - tooltip_rect.width())
            if tooltip_rect.left() < screen_rect.left():
                global_pos.setX(screen_rect.left())

        self.custom_tooltip.move(global_pos)
        self.custom_tooltip.show()
        self.custom_tooltip.raise_()  # Asegura que el tooltip se muestre sobre otros elementos


# ================================================================
# Diálogo Checklist (Con Estilos Aplicados)
# ================================================================
class ChecklistDialog(QtWidgets.QDialog):
    def __init__(self, tipo_herramental, checklist_items, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Checklist Mantenimiento") # Título más corto
        self.setMinimumWidth(500) # Un poco más ancho
        self.setModal(True)
        # Aplicar estilos base al diálogo
        self.setStyleSheet(GENERAL_STYLESHEET + """
            QDialog { background-color: #FDFDFD; }
            QScrollArea { border: none; }
            QCheckBox { padding: 4px 0px; /* Espacio vertical entre checkboxes */ }
        """)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15) # Más margen interno
        layout.setSpacing(15) # Más espacio entre elementos

        title_label = QtWidgets.QLabel(f"Verificar para: {tipo_herramental}")
        title_label.setStyleSheet("font-size: 14pt; font-weight: bold; color: #333; margin-bottom: 5px;")
        layout.addWidget(title_label)

        # --- Área de Scroll para Checkboxes ---
        scroll_area = QtWidgets.QScrollArea(self)
        scroll_area.setWidgetResizable(True)
        scroll_area.setStyleSheet("background-color: white; border: 1px solid #DDD;") # Fondo blanco para scroll
        scroll_content = QtWidgets.QWidget()
        scroll_content.setStyleSheet("background-color: white;") # Fondo blanco interior
        self.checklist_layout = QtWidgets.QVBoxLayout(scroll_content)
        self.checklist_layout.setContentsMargins(10, 10, 10, 10)
        self.checklist_layout.setSpacing(8)
        scroll_content.setLayout(self.checklist_layout)
        scroll_area.setWidget(scroll_content)
        layout.addWidget(scroll_area) # Añadir scroll area

        # --- Llenar Checkboxes ---
        self.checkboxes = []
        if not checklist_items:
             no_items_label = QtWidgets.QLabel("   No hay puntos de checklist definidos para este tipo.")
             no_items_label.setStyleSheet("color: grey; font-style: italic;")
             self.checklist_layout.addWidget(no_items_label)
             self.all_checked = True # Se considera completado si no hay items
        else:
            self.all_checked = False
            for item_text in checklist_items:
                checkbox = QtWidgets.QCheckBox(item_text)
                checkbox.stateChanged.connect(self._check_completion) # Renombrar a método privado
                self.checklist_layout.addWidget(checkbox)
                self.checkboxes.append(checkbox)
            self.checklist_layout.addStretch() # Empujar checkboxes hacia arriba si son pocos

        # --- Botones ---
        self.button_box = QtWidgets.QDialogButtonBox(QtCore.Qt.Horizontal) # Orientación horizontal
        self.continue_button = self.button_box.addButton("Continuar", QtWidgets.QDialogButtonBox.AcceptRole)
        self.cancel_button = self.button_box.addButton("Cancelar", QtWidgets.QDialogButtonBox.RejectRole)

        # Aplicar estilos verde/rojo
        self.continue_button.setStyleSheet(BTN_STYLE_ACCEPT)
        self.cancel_button.setStyleSheet(BTN_STYLE_REJECT)

        self.continue_button.setEnabled(self.all_checked) # Habilitar/deshabilitar inicialmente

        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box, 0, QtCore.Qt.AlignRight) # Alinear botones a la derecha

        self._check_completion() # Llamada inicial

    def _check_completion(self):
        """Método privado para verificar si todos los checkboxes están marcados."""
        if not self.checkboxes:
             self.all_checked = True
        else:
            self.all_checked = all(cb.isChecked() for cb in self.checkboxes)
        self.continue_button.setEnabled(self.all_checked)

    # No es necesario is_completed, el estado se maneja internamente y con accept/reject


# ================================================================
# Diálogo Gestionar Checklists (Con Estilos Aplicados)
# ================================================================
class ManageChecklistsDialog(QtWidgets.QDialog):
    def __init__(self, db_path, checklist_path, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Gestionar Checklists por Tipo de Herramental")
        self.db_path = db_path
        self.checklist_path = checklist_path
        self.checklists_data = self._load_checklists() # Renombrar a privado
        self.tipos_herramental = self._get_tipos_herramental() # Renombrar a privado
        self.current_tipo = None
        self.changes_made = False # Flag para saber si hay cambios sin guardar
        self.setMinimumSize(650, 450) # Ligeramente más grande
        # Aplicar estilos base al diálogo
        self.setStyleSheet(GENERAL_STYLESHEET + """
             QDialog { background-color: #FDFDFD; }
             QGroupBox { font-weight: bold; margin-top: 10px; }
             QListWidget { background-color: white; border: 1px solid #ccc; }
         """)

        main_layout = QtWidgets.QVBoxLayout(self)
        main_layout.setSpacing(12)

        # --- Selección de Tipo ---
        selection_layout = QtWidgets.QHBoxLayout()
        label_tipo = QtWidgets.QLabel("Tipo de Herramental:")
        label_tipo.setStyleSheet("font-weight: bold;") # Negrita para el label
        selection_layout.addWidget(label_tipo)
        self.combo_tipo = QtWidgets.QComboBox()
        self.combo_tipo.setMinimumWidth(250) # Ancho mínimo para el combo
        self.combo_tipo.addItems(["Seleccione un tipo..."] + sorted(self.tipos_herramental))
        self.combo_tipo.currentIndexChanged.connect(self._load_checklist_for_selected_tipo) # Privado
        selection_layout.addWidget(self.combo_tipo, 1) # Stretch factor
        main_layout.addLayout(selection_layout)

        # --- Lista y Edición ---
        edit_area_layout = QtWidgets.QHBoxLayout() # Layout horizontal para lista y botones de edición

        # Columna Izquierda: Lista
        list_group = QtWidgets.QGroupBox("Puntos del Checklist")
        list_layout = QtWidgets.QVBoxLayout(list_group)
        self.list_widget = QtWidgets.QListWidget()
        self.list_widget.itemSelectionChanged.connect(self._enable_disable_buttons) # Habilitar/deshabilitar botón de borrar
        list_layout.addWidget(self.list_widget)
        edit_area_layout.addWidget(list_group, 2) # Darle más espacio (factor 2)

        # Columna Derecha: Añadir/Eliminar
        edit_buttons_layout = QtWidgets.QVBoxLayout()
        edit_buttons_layout.setSpacing(8)
        edit_buttons_layout.setAlignment(QtCore.Qt.AlignTop) # Alinear arriba

        self.new_item_edit = QtWidgets.QLineEdit()
        self.new_item_edit.setPlaceholderText("Nuevo punto...")
        edit_buttons_layout.addWidget(self.new_item_edit)

        self.btn_add = QtWidgets.QPushButton("Añadir Punto")
        self.btn_add.setIcon(QtGui.QIcon.fromTheme("list-add", QtGui.QIcon(":/qt-project.org/styles/commonstyle/images/add-16.png"))) # Icono estándar
        self.btn_add.setStyleSheet(BTN_STYLE_ACCEPT) # Verde
        self.btn_add.clicked.connect(self._add_item) # Privado
        edit_buttons_layout.addWidget(self.btn_add)

        self.btn_edit = QtWidgets.QPushButton("Editar Punto")
        self.btn_edit.setIcon(QtGui.QIcon.fromTheme("document-edit", QtGui.QIcon(":/qt-project.org/styles/commonstyle/images/edit-16.png")))
        self.btn_edit.setStyleSheet(BTN_STYLE_EDIT)  # Azul, estilo nuevo para edición
        self.btn_edit.clicked.connect(self._edit_item)  # Función privada para editar
        edit_buttons_layout.addWidget(self.btn_edit)

        self.btn_remove = QtWidgets.QPushButton("Eliminar Punto")
        self.btn_remove.setIcon(QtGui.QIcon.fromTheme("list-remove", QtGui.QIcon(":/qt-project.org/styles/commonstyle/images/remove-16.png"))) # Icono estándar
        self.btn_remove.setStyleSheet(BTN_STYLE_REJECT) # Rojo
        self.btn_remove.clicked.connect(self._remove_item) # Privado
        edit_buttons_layout.addWidget(self.btn_remove)

        edit_area_layout.addLayout(edit_buttons_layout, 1) # Espacio (factor 1)
        main_layout.addLayout(edit_area_layout)

        # --- Botones Guardar/Cerrar ---
        bottom_button_layout = QtWidgets.QHBoxLayout()
        bottom_button_layout.addStretch() # Empujar a la derecha
        self.btn_save = QtWidgets.QPushButton("Guardar Cambios")
        self.btn_save.setIcon(QtGui.QIcon.fromTheme("document-save", QtGui.QIcon(":/qt-project.org/styles/commonstyle/images/save-16.png"))) # Icono estándar
        self.btn_save.setStyleSheet(BTN_STYLE_ACCEPT) # Verde
        self.btn_save.clicked.connect(self._save_changes) # Privado
        bottom_button_layout.addWidget(self.btn_save)

        self.btn_close = QtWidgets.QPushButton("Cerrar")
        # No necesita icono, es estándar
        self.btn_close.setStyleSheet(BTN_STYLE_REJECT) # Rojo (ya que cierra sin guardar cambios no guardados)
        self.btn_close.clicked.connect(self.close) # Usar close para el manejo de eventos
        bottom_button_layout.addWidget(self.btn_close)
        main_layout.addLayout(bottom_button_layout)

        # Estado inicial de los botones/widgets
        self._enable_disable_buttons()

    # --- Métodos Helper Privados ---
    def _get_tipos_herramental(self):
        tipos = set() # Usar set para evitar duplicados iniciales
        try:
            # Leer solo la columna necesaria, puede ser más rápido
            df = pd.read_csv(self.db_path, encoding='utf-8-sig', usecols=["TIPO DE HERRAMENTAL"], low_memory=False)
            # Iterar y limpiar
            for tipo in df["TIPO DE HERRAMENTAL"].dropna():
                 tipo_limpio = str(tipo).strip()
                 if tipo_limpio: # Añadir solo si no está vacío después de limpiar
                      tipos.add(tipo_limpio)
            print(f"Tipos de herramental encontrados: {len(tipos)}")
            return sorted(list(tipos)) # Convertir a lista ordenada
        except FileNotFoundError:
             QtWidgets.QMessageBox.critical(self, "Error Fatal", f"No se encontró el archivo DB:\n{self.db_path}")
             return []
        except KeyError:
             QtWidgets.QMessageBox.critical(self, "Error Fatal", f"La columna 'TIPO DE HERRAMENTAL' no existe en:\n{self.db_path}")
             return []
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Error", f"No se pudo leer los tipos de herramental del CSV:\n{e}")
            return []

    def _load_checklists(self):
        try:
            with open(self.checklist_path, 'r', encoding='utf-8-sig') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Archivo {self.checklist_path} no encontrado. Creando uno vacío.")
            return {}
        except json.JSONDecodeError:
             QtWidgets.QMessageBox.critical(self, "Error", f"Archivo JSON corrupto:\n{self.checklist_path}")
             return {}
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Error al cargar checklists:\n{e}")
            return {}

    def _save_checklists(self):
        try:
            # Ordenar tipos alfabéticamente y puntos dentro de cada tipo
            sorted_data = {}
            for tipo in sorted(self.checklists_data.keys()):
                 sorted_data[tipo] = sorted(self.checklists_data[tipo])

            with open(self.checklist_path, 'w', encoding='utf-8-sig') as f:
                # Usar indent=2 para legibilidad en el JSON
                json.dump(sorted_data, f, indent=2, ensure_ascii=False)
            self.changes_made = False # Resetear flag de cambios
            self._enable_disable_buttons() # Actualizar estado del botón guardar
            print(f"Checklists guardados en {self.checklist_path}")
            return True
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo guardar el archivo de checklists:\n{e}")
            return False

    def _load_checklist_for_selected_tipo(self):
        """Carga la lista de checklist cuando se cambia el ComboBox."""
        if self.changes_made:
             reply = self._ask_save_changes()
             if reply == QtWidgets.QMessageBox.Cancel:
                  # Si cancela, volver al tipo anterior en el combo
                  if self.current_tipo:
                       index = self.combo_tipo.findText(self.current_tipo)
                       if index >= 0:
                            self.combo_tipo.setCurrentIndex(index)
                  else:
                       self.combo_tipo.setCurrentIndex(0)
                  return # No continuar cambiando
             elif reply == QtWidgets.QMessageBox.Save:
                  if not self._save_changes(): # Si falla el guardado, no continuar
                       return

        # Proceder a cargar el nuevo tipo
        index = self.combo_tipo.currentIndex()
        if index == 0:
            self.current_tipo = None
            self.list_widget.clear()
        else:
            self.current_tipo = self.combo_tipo.currentText()
            self._populate_list_widget()

        self._enable_disable_buttons() # Habilitar/deshabilitar widgets de edición

    def _populate_list_widget(self):
        """Llena el QListWidget con los items del checklist actual."""
        self.list_widget.clear()
        if self.current_tipo and self.current_tipo in self.checklists_data:
            items = self.checklists_data.get(self.current_tipo, []) # Usar get con default
            self.list_widget.addItems(items) # AddItems es más eficiente

    def _add_item(self):
        """Añade un nuevo item al checklist actual."""
        if not self.current_tipo: return
        new_text = self.new_item_edit.text().strip()
        if not new_text:
            QtWidgets.QMessageBox.warning(self, "Campo Vacío", "Ingrese el texto para el nuevo punto.")
            return

        # Verificar si ya existe (insensible a mayúsculas/minúsculas)
        current_items_lower = [item.lower() for item in self.checklists_data.get(self.current_tipo, [])]
        if new_text.lower() in current_items_lower:
             QtWidgets.QMessageBox.information(self, "Duplicado", f"El punto '{new_text}' ya existe para este tipo.")
             return

        # Añadir al modelo de datos interno
        if self.current_tipo not in self.checklists_data:
            self.checklists_data[self.current_tipo] = []
        self.checklists_data[self.current_tipo].append(new_text)
        self.changes_made = True

        # Actualizar la UI
        self.list_widget.addItem(new_text) # Añadir a la lista visual
        self.list_widget.sortItems() # Mantener ordenado alfabéticamente
        self.new_item_edit.clear() # Limpiar campo de entrada
        self.new_item_edit.setFocus() # Poner foco de nuevo para añadir más
        self._enable_disable_buttons() # Actualizar botones (guardar)

    def _edit_item(self):
        """Edita el item seleccionado del checklist actual."""
        if not self.current_tipo:
            return
        selected_items = self.list_widget.selectedItems()
        if not selected_items:
            return  # No hay ningún item seleccionado

        # Obtener el item seleccionado (se asume selección simple)
        current_item = selected_items[0]
        current_text = current_item.text()

        # Solicitar el nuevo texto usando QInputDialog con el texto actual como valor por defecto
        new_text, ok = QtWidgets.QInputDialog.getText(
            self,
            "Editar Punto",
            "Modificar el punto:",
            QtWidgets.QLineEdit.Normal,
            current_text
        )
        if not ok:
            return  # El usuario canceló el cambio

        new_text = new_text.strip()
        if not new_text:
            QtWidgets.QMessageBox.warning(self, "Campo Vacío", "El texto no puede estar vacío.")
            return

        # Verificar que el nuevo texto no se duplique en el checklist actual
        # Ignoramos la comparación del mismo item que se está editando
        checklist = self.checklists_data.get(self.current_tipo, [])
        current_items_lower = [item.lower() for item in checklist]
        if new_text.lower() != current_text.lower() and new_text.lower() in current_items_lower:
            QtWidgets.QMessageBox.information(self, "Duplicado", f"El punto '{new_text}' ya existe para este tipo.")
            return

        # Actualizar el modelo de datos interno
        try:
            index = checklist.index(current_text)
            checklist[index] = new_text
        except ValueError:
            QtWidgets.QMessageBox.warning(self, "Error", "No se encontró el elemento en los datos internos.")
            return

        # Actualizar la UI: actualizar el texto del item seleccionado y reordenar la lista
        current_item.setText(new_text)
        self.list_widget.sortItems()  # Garantizar que la lista siga ordenada
        self.changes_made = True
        self._enable_disable_buttons()  # Actualizar el estado de botones según corresponda

    def _remove_item(self):
        """Elimina el item seleccionado del checklist actual."""
        if not self.current_tipo: return
        selected_items = self.list_widget.selectedItems()
        if not selected_items: return # No hay nada seleccionado

        selected_item = selected_items[0]
        item_text = selected_item.text()

        # Usar QMessageBox manual para botones en español y estilo
        msg_box = QtWidgets.QMessageBox(self)
        msg_box.setWindowTitle("Confirmar Eliminación")
        msg_box.setText(f"¿Seguro que quieres eliminar:\n'{item_text}'\n\ndel checklist para '{self.current_tipo}'?")
        msg_box.setIcon(QtWidgets.QMessageBox.Warning)
        btn_si = msg_box.addButton("Sí, eliminar", QtWidgets.QMessageBox.YesRole)
        btn_no = msg_box.addButton("No", QtWidgets.QMessageBox.NoRole)
        btn_si.setStyleSheet(BTN_STYLE_REJECT) # Rojo para eliminar
        btn_no.setStyleSheet("QPushButton { color: black; font-weight: bold; }") # Estilo default
        msg_box.setDefaultButton(btn_no)
        msg_box.exec_()

        if msg_box.clickedButton() == btn_si:
            # Eliminar del modelo de datos interno
            if self.current_tipo in self.checklists_data:
                 try:
                     self.checklists_data[self.current_tipo].remove(item_text)
                     self.changes_made = True
                     # Opcional: eliminar la clave si la lista queda vacía
                     # if not self.checklists_data[self.current_tipo]:
                     #     del self.checklists_data[self.current_tipo]
                     #     print(f"Tipo '{self.current_tipo}' eliminado por estar vacío.")
                 except ValueError:
                     print(f"Advertencia: El item '{item_text}' no se encontró en la lista interna al intentar eliminar.")

            # Eliminar de la UI (tomar item por puntero)
            self.list_widget.takeItem(self.list_widget.row(selected_item))
            self._enable_disable_buttons() # Actualizar botones (guardar, eliminar)


    def _save_changes(self):
        """Guarda los cambios realizados en el archivo JSON."""
        if not self.changes_made:
             QtWidgets.QMessageBox.information(self, "Sin Cambios", "No hay cambios pendientes de guardar.")
             return False # Indicar que no se guardó porque no había cambios

        if self._save_checklists(): # Llama a la función que escribe el archivo
            QtWidgets.QMessageBox.information(self, "Guardado", "Checklists guardados correctamente.")
            return True # Indicar éxito
        else:
             # _save_checklists ya mostró el error
             return False # Indicar fallo

    def _enable_disable_buttons(self):
        """Actualiza el estado habilitado/deshabilitado de los botones y widgets."""
        is_tipo_selected = self.current_tipo is not None
        is_item_selected = len(self.list_widget.selectedItems()) > 0

        self.list_widget.setEnabled(is_tipo_selected)
        self.new_item_edit.setEnabled(is_tipo_selected)
        self.btn_add.setEnabled(is_tipo_selected)
        self.btn_remove.setEnabled(is_tipo_selected and is_item_selected)
        self.btn_save.setEnabled(self.changes_made) # Habilitar guardar solo si hay cambios

    def _ask_save_changes(self):
        """Pregunta al usuario si desea guardar los cambios antes de cerrar o cambiar."""
        msg_box = QtWidgets.QMessageBox(self)
        msg_box.setWindowTitle("Guardar Cambios")
        msg_box.setText("Hay cambios sin guardar en el checklist actual.")
        msg_box.setInformativeText("¿Deseas guardar los cambios?")
        msg_box.setIcon(QtWidgets.QMessageBox.Warning)
        # Usar botones estándar traducidos si es posible, o añadir manuales
        btn_save = msg_box.addButton("Guardar", QtWidgets.QMessageBox.SaveRole)
        btn_discard = msg_box.addButton("Descartar", QtWidgets.QMessageBox.DiscardRole)
        btn_cancel = msg_box.addButton("Cancelar", QtWidgets.QMessageBox.CancelRole)
        # Aplicar estilos
        btn_save.setStyleSheet(BTN_STYLE_ACCEPT)
        btn_discard.setStyleSheet(BTN_STYLE_REJECT)
        btn_cancel.setStyleSheet("QPushButton { color: black; font-weight: bold; }")

        return msg_box.exec_() # Devuelve el botón presionado (Save, Discard, Cancel)


    # Sobreescribir closeEvent para preguntar si guardar cambios
    def closeEvent(self, event):
        """Se ejecuta cuando el usuario intenta cerrar la ventana."""
        if self.changes_made:
            reply = self._ask_save_changes()

            if reply == QtWidgets.QMessageBox.Save:
                if self._save_changes():
                     event.accept() # Aceptar cierre si se guardó bien
                else:
                     event.ignore() # Ignorar cierre si falló el guardado
            elif reply == QtWidgets.QMessageBox.Discard:
                 event.accept() # Aceptar cierre descartando cambios
            else: # Cancel
                 event.ignore() # Ignorar el evento de cierre
        else:
            event.accept() # No hay cambios, cerrar normalmente


# --- FIN NUEVA CLASE: ManageChecklistsDialog ---

###############################################################################
# Clase Window4Page (sección de mantenimiento)
###############################################################################
class Window4Page(QtWidgets.QWidget):
    def __init__(self, user_alias=None, parent=None):
        super().__init__(parent)
        # Si no se pasó un alias, se usa el valor almacenado en Session.user_alias.
        self.user_alias = user_alias or Session.user_alias
        print("Window4Page, user_alias recibido:", self.user_alias)

        # Layout principal
        main_layout = QtWidgets.QVBoxLayout(self)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)
        
        # Encabezado con título y botón de alerta
        header_layout = QtWidgets.QHBoxLayout()
        header = QtWidgets.QLabel("Control de Mantenimiento de Herramental")
        header.setStyleSheet("font-weight: bold; font-size: 18px;")
        header.setAlignment(QtCore.Qt.AlignCenter)
        header_layout.addWidget(header)
        header_layout.addStretch()
        self.btn_alerta = QtWidgets.QPushButton("Alertas")
        self.btn_alerta.setToolTip("Muestra las alertas relacionadas con el mantenimiento de herramental.")
        self.btn_alerta.setFixedSize(140, 40)
        self.btn_alerta.setStyleSheet("border: none; padding: 5px;")
        header_layout.addWidget(self.btn_alerta)
        main_layout.addLayout(header_layout)

        # Conexión para mostrar detalles cuando se haga clic en el botón de alerta
        self.btn_alerta.clicked.connect(self.show_alert_details)
        
        self.filename = DB_PATH
        self.current_item = None

        # Área de Búsqueda (no se utiliza Combobox, puesto que la búsqueda es mixta)
        search_layout = QtWidgets.QHBoxLayout()
        # La búsqueda se realiza en ambos campos a la vez; el placeholder lo indica
        self.search_field = QtWidgets.QLineEdit()
        self.search_field.setPlaceholderText("Ingrese búsqueda (Nomenclatura o Job)...")
        self.search_field.setStyleSheet("background-color: #F0F8FF; margin: 5px; padding: 5px; "
                                        "border: 2px solid #007ACC; border-radius: 5px;")
        self.search_field.returnPressed.connect(self.search_item)
        search_layout.addWidget(self.search_field)
        
        btn_search = QtWidgets.QPushButton("Buscar")
        btn_search.setToolTip("Inicia la búsqueda en Nomenclatura y Job.")
        btn_search.setStyleSheet("background-color: #FFA07A; color: white; margin: 5px; padding: 5px; border-radius: 5px;")
        btn_search.clicked.connect(self.search_item)
        search_layout.addWidget(btn_search)
        
        self.btn_refresh = QtWidgets.QPushButton("Refresh")
        self.btn_refresh.setToolTip("Refresca los datos del listado.")
        self.btn_refresh.setStyleSheet("background-color: #007ACC; color: white; margin: 5px; padding: 5px; border-radius: 5px;")
        self.btn_refresh.clicked.connect(self.refresh_data)
        search_layout.addWidget(self.btn_refresh)
                
        self.btn_manage_checklists = QtWidgets.QPushButton("Gestionar Checklists")
        self.btn_manage_checklists.setToolTip("Abre la ventana para editar los checklists por tipo de herramental.")
        self.btn_manage_checklists.setStyleSheet("background-color: #6c757d; color: white; margin: 5px; padding: 8px; border-radius: 5px;")
        self.btn_manage_checklists.clicked.connect(self.open_manage_checklists_dialog)
        main_layout.addWidget(self.btn_manage_checklists)
        main_layout.addLayout(search_layout)
        
        # Área de Información de Mantenimiento
        self.info_frame = QtWidgets.QFrame()
        self.info_frame.setFrameShape(QtWidgets.QFrame.StyledPanel)
        info_layout = QtWidgets.QFormLayout(self.info_frame)
        self.lblNomenclatura = QtWidgets.QLabel("No disponible")
        self.lblUltimoMantenimiento = QtWidgets.QLabel("No disponible")
        self.lblProximoMantenimiento = QtWidgets.QLabel("No disponible")
        self.lblStatus = QtWidgets.QLabel("No disponible")
        self.lblStatusSurtido = QtWidgets.QLabel("No disponible")
        info_layout.addRow("Nomenclatura:", self.lblNomenclatura)
        info_layout.addRow("Último Mantenimiento:", self.lblUltimoMantenimiento)
        info_layout.addRow("Próximo Mantenimiento:", self.lblProximoMantenimiento)
        info_layout.addRow("Status:", self.lblStatus)
        info_layout.addRow("Status surtido:", self.lblStatusSurtido)
        main_layout.addWidget(self.info_frame)
        
        # Botones de acción
        self.btn_mantenimiento = HoverHelpButton("Realizar Mantenimiento",
                                                   "Registra un nuevo mantenimiento para el herramental seleccionado.",
                                                   self)
        self.btn_mantenimiento.setStyleSheet("background-color: #4CAF50; color: white; margin: 5px; "
                                               "padding: 8px; border-radius: 5px;")
        self.btn_mantenimiento.clicked.connect(self.perform_maintenance)
        main_layout.addWidget(self.btn_mantenimiento)
        
        self.btn_editar = HoverHelpButton("Editar Mantenimiento",
                                          "Permite editar la información del mantenimiento.",
                                          self)
        self.btn_editar.setStyleSheet("background-color: #2196F3; color: white; margin: 5px; "
                                      "padding: 8px; border-radius: 5px;")
        self.btn_editar.clicked.connect(self.edit_maintenance)
        main_layout.addWidget(self.btn_editar)
        
        self.btn_MPI = HoverHelpButton("MPI", "Abre el documento MPI relacionado.", self)
        self.btn_MPI.setStyleSheet("background-color: #FF8C00; color: white; margin: 5px; "
                                   "padding: 8px; border-radius: 5px;")
        self.btn_MPI.clicked.connect(self.open_MPI_pdf)
        main_layout.addWidget(self.btn_MPI)
        
        # Layout para botones de exportación
        Export_buttons_layout = QtWidgets.QHBoxLayout()        
        self.btn_export = HoverHelpButton("Exportar", "Exportar Listado de Herramentales a Excel", self)
        self.btn_export.setFixedSize(160, 40)
        self.btn_export.setStyleSheet("background-color: #FFA500; color: white; margin: 5px; "
                                      "padding: 8px; border-radius: 5px; font-size: 10px;")
        self.btn_export.clicked.connect(self.export_maintenance_list)
        Export_buttons_layout.addWidget(self.btn_export)
        
        self.btn_export_template = HoverHelpButton(
            "Export Template",
            "Copia de la plantilla (UPDATE_MANTENIMIENTO_HERRAMENTALES_TOOLTRACK+.csv) con encabezados obligatorios resaltados.",
            self)
        self.btn_export_template.setFixedSize(160, 40)
        self.btn_export_template.setStyleSheet("background-color: #FFA500; color: white; margin: 5px; "
                                               "padding: 8px; border-radius: 5px; font-size: 10px;")
        self.btn_export_template.clicked.connect(self.export_template)
        Export_buttons_layout.addWidget(self.btn_export_template)
        main_layout.addLayout(Export_buttons_layout)

        self.update_alert_button_icon()
        # Inicializa el autocompletado mixto
        self.updateCompleter()

    def updateCompleter(self):
        try:
            df = pd.read_csv(self.filename, encoding='utf-8-sig')
            # Excluir los registros cuyo 'TIPO DE HERRAMENTAL' sea 'CONSUMABLE'
            if "TIPO DE HERRAMENTAL" in df.columns:
                df = df[df["TIPO DE HERRAMENTAL"].astype(str).str.lower() != "consumable"]

            suggestions = []
            for col in ["NOMENCLATURA", "JOB"]:
                if col in df.columns:
                    suggestions.extend(df[col].dropna().astype(str).unique().tolist())
            # Eliminar duplicados y ordenar
            suggestions = sorted(set(suggestions))
        except Exception as e:
            print("Error al actualizar el completer:", e)
            suggestions = []

        # Se utiliza un modelo de QStringListModel para el QCompleter
        model = QtCore.QStringListModel()
        model.setStringList(suggestions)
        completer = QtWidgets.QCompleter()
        completer.setModel(model)
        completer.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
        # Configurar el filtro para que busque coincidencias en cualquier parte de la cadena
        completer.setFilterMode(QtCore.Qt.MatchContains)
        self.search_field.setCompleter(completer)

    
    def search_item(self):
        # Obtener el valor de búsqueda del QLineEdit
        search_value = self.search_field.text().strip()
        if not search_value:
            QtWidgets.QMessageBox.warning(self, "Aviso", "Por favor ingresa un valor para buscar.")
            return

        try:
            df = pd.read_csv(self.filename, encoding='utf-8-sig')
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo leer el CSV:\n{e}")
            return

        # Excluir los registros cuyo 'TIPO DE HERRAMENTAL' sea 'CONSUMABLE'
        if "TIPO DE HERRAMENTAL" in df.columns:
            df = df[df["TIPO DE HERRAMENTAL"].astype(str).str.lower() != "consumable"]

        # Crear una máscara que combina la búsqueda en las columnas 'NOMENCLATURA' y 'JOB'
        mask = pd.Series(False, index=df.index)
        if "NOMENCLATURA" in df.columns:
            mask = mask | df["NOMENCLATURA"].astype(str).str.contains(search_value, case=False, na=False)
        if "JOB" in df.columns:
            mask = mask | df["JOB"].astype(str).str.contains(search_value, case=False, na=False)

        df_result = df[mask]

        if df_result.empty:
            QtWidgets.QMessageBox.information(self, "Resultado", "No encontrado")
            self.current_item = None
            self.lblUltimoMantenimiento.setText("No encontrado")
            self.lblProximoMantenimiento.setText("No encontrado")
            self.lblStatus.setText("No encontrado")
        else:
            # Selecciona el primer ítem encontrado
            item = df_result.iloc[0]
            # Si el ítem es de tipo MULTIPLE o su tipo de consumible es CONSUMABLE, no se muestra el mantenimiento
            if (str(item.get("TYPE_INOUT", "")).strip().upper() == "MULTIPLE" or 
                str(item.get("TYPE_CONS_INOUT", "")).strip().upper() == "CONSUMABLE"):
                QtWidgets.QMessageBox.information(self, "Búsqueda", 
                                                "El ítem seleccionado no tiene registro de mantenimiento debido a que es un consumible.")
                self.current_item = None
                return
            self.current_item = item
            self.update_info_display()
            self.update_alert_button_icon()
    
    # ==============================================================================
    # Método para exportar el listado de herramentales (filtrado por TYPE_INOUT "SINGLE") a Excel.
    # ==============================================================================
    def export_maintenance_list(self):
        try:
            df = pd.read_csv(DB_PATH, encoding="utf-8-sig")
            # Columnas que se desean exportar.
            desired_columns = ["HERRAMENTAL_ID", "ITEM_TYPE", "NOMENCLATURA", "JOB",
                               "TIPO DE HERRAMENTAL", "PROYECTO", "ULTIMO_MANTENIMIENTO",
                               "PROXIMO_MANTENIMIENTO", "PERIODO", "STATUS", "DIAS_ALERTA"]
            available_cols = [col for col in desired_columns if col in df.columns]
            df_export = df[available_cols]
            # Filtrar ítems con TYPE_INOUT == "SINGLE"
            if "TYPE_INOUT" in df.columns:
                df_export = df_export[df["TYPE_INOUT"].astype(str).str.upper() == "SINGLE"]
            df_export = df_export.astype(str)
            desktop = os.path.join(os.path.expanduser("~"), "Desktop")
            export_path = os.path.join(desktop, "Mantenimiento_Herramentales_ToolTrack.xlsx")
            df_export.to_excel(export_path, index=False)
            QtWidgets.QMessageBox.information(self, "Exportación exitosa", f"Listado exportado a:\n{export_path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Error al exportar el listado:\n{e}")
    
# ==============================================================================
# Método para exportar la plantilla (template) en Excel con encabezados en amarillo,
# validaciones y sin incluir la columna "HERRAMENTAL_ID". El archivo se guarda en
# el escritorio predeterminado del usuario.
# ==============================================================================
    def export_template(self):
        try:
            import os
            import pandas as pd

            # Intentar obtener la ruta del escritorio usando la API de Windows
            try:
                from win32com.shell import shell, shellcon
                desktop_path = shell.SHGetFolderPath(0, shellcon.CSIDL_DESKTOP, None, 0)
            except ImportError:
                # Fallback si no se tiene pywin32 instalado
                desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")

            # Ruta del archivo CSV de plantilla
            template_csv_path = r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\UPDATE_MANTENIMIENTO_HERRAMENTALES_TOOLTRACK+.csv"
            df_template = pd.read_csv(template_csv_path, encoding="utf-8-sig")
            df_template = df_template.fillna("")
            
            # Definir el orden de columnas a exportar (se omite "HERRAMENTAL_ID")
            columns_to_export = [
                "ITEM_TYPE", "NOMENCLATURA", "JOB", "TIPO DE HERRAMENTAL", "LADO",
                "PROYECTO", "RACK", "ULTIMO_MANTENIMIENTO", "PROXIMO_MANTENIMIENTO",
                "PERIODO", "STATUS", "DIAS_ALERTA", "MPI", "TYPE_INOUT", "TYPE_CONS_INOUT",
                "STATUS_INOUT", "USER_OUT", "MULTI_STOCK_IN", "MULTI_STOCK_OUT",
                "MULTI_STOCK_ALL", "LAST_OUT", "EMPLOYEE_OUT"
            ]
            df_template = df_template[columns_to_export]
            
            # Lista de columnas obligatorias (encabezados a resaltar en amarillo)
            mandatory_cols = [
                "ITEM_TYPE", "NOMENCLATURA", "JOB",
                "TIPO DE HERRAMENTAL", "PROYECTO", "TYPE_INOUT", "STATUS_INOUT"
            ]
            
            # Definir la ruta de exportación en el escritorio predeterminado
            export_path = os.path.join(desktop_path, "UPDATE_MANTENIMIENTO_HERRAMENTALES_TOOLTRACK+.xlsx")
            
            # Crear ExcelWriter utilizando xlsxwriter
            writer = pd.ExcelWriter(export_path, engine="xlsxwriter")
            df_template.to_excel(writer, sheet_name="Template", index=False)
            workbook  = writer.book
            worksheet = writer.sheets["Template"]
            
            # Formato para encabezados obligatorios: fondo amarillo, bold y borde
            header_format = workbook.add_format({
                'bg_color': '#FFFF00',
                'bold': True,
                'border': 1
            })
            for col_num, col_name in enumerate(df_template.columns.values):
                if col_name in mandatory_cols:
                    worksheet.write(0, col_num, col_name, header_format)
            
            # Agregar validación para ITEM_TYPE (columna A) con opciones "C" y "NC"
            worksheet.data_validation("A2:A1048576", {
                'validate': 'list',
                'source': ['C', 'NC'],
                'input_title': 'ITEM_TYPE',
                'input_message': 'Seleccione: C (Consumibles) o NC (Herramentales, no consumibles)'
            })
            # Agregar comentario explicativo en el encabezado de ITEM_TYPE
            worksheet.write_comment("A1", "C: Consumibles\nNC: Herramentales (no consumibles)")
            
            # Agregar validación para TYPE_INOUT (columna N: 14ª columna)
            worksheet.data_validation("N2:N1048576", {
                'validate': 'list',
                'source': ['SINGLE', 'MULTIPLE'],
                'input_title': 'TYPE_INOUT',
                'input_message': 'Seleccione: SINGLE o MULTIPLE'
            })
            
            # Agregar validación para STATUS_INOUT (columna P: 16ª columna)
            worksheet.data_validation("P2:P1048576", {
                'validate': 'list',
                'source': ['in', 'out', 'area roja', 'scrap'],
                'input_title': 'STATUS_INOUT',
                'input_message': 'Seleccione: in, out, area roja o scrap'
            })
            
            writer.close()
            QtWidgets.QMessageBox.information(self, "Exportación exitosa",
                                            f"Plantilla exportada a:\n{export_path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error",
                                        f"Error al exportar la plantilla:\n{e}")

# --- MÉTODO update_info_display ACTUALIZADO ---
    def update_info_display(self):
        """
        Actualiza las etiquetas de información en la UI basadas en self.current_item.
        Calcula próximo mantenimiento, status y ofrece acciones si no hay registro previo.
        """
        # Estilo común para botones en QMessageBox dentro de este método
        button_style = "QPushButton { color: black; font-weight: bold; min-width: 80px; padding: 5px; }"

        if self.current_item is None:
            # Limpiar todos los labels si no hay item seleccionado
            self.lblNomenclatura.setText("N/A")
            self.lblUltimoMantenimiento.setText("N/A")
            self.lblProximoMantenimiento.setText("N/A")
            self.lblStatus.setText("No Seleccionado")
            self.lblStatus.setStyleSheet("") # Sin estilo especial
            self.lblStatusSurtido.setText("N/A")
            self.lblStatusSurtido.setStyleSheet("")
            # Deshabilitar botones que dependen de un item? (Opcional)
            # self.btn_mantenimiento.setEnabled(False)
            # self.btn_editar.setEnabled(False)
            # self.btn_MPI.setEnabled(False)
            return

        # Habilitar botones si estaban deshabilitados (opcional)
        # self.btn_mantenimiento.setEnabled(True)
        # self.btn_editar.setEnabled(True)
        # self.btn_MPI.setEnabled(True)
        nomenclatura = self.current_item.get("NOMENCLATURA", "No disponible")
        self.lblNomenclatura.setText(nomenclatura)
        ultimo_date = None
        proximo_date_calculated = None
        status = "Error" # Estado por defecto
        color = "lightcoral" # Color por defecto (rojo suave)

        try:
            # Usar .get() para evitar KeyError si la columna falta
            ultimo_str = self.current_item.get("ULTIMO_MANTENIMIENTO", "")

            # Comprobar si es N/A, vacío, o nulo en pandas
            is_na_or_empty = pd.isnull(ultimo_str) or str(ultimo_str).strip().upper() == "N/A" or not str(ultimo_str).strip()

            if is_na_or_empty:
                # --- Caso: Sin registro previo ---
                self.lblUltimoMantenimiento.setText("N/A")
                self.lblProximoMantenimiento.setText("N/A")
                status = "REQUIERE REGISTRO"
                color = "orange"
                self.lblStatus.setText(status)
                self.lblStatus.setStyleSheet(f"background-color: {color}; padding: 5px; border-radius: 3px;")

                # Actualizar Status Surtido también en este caso
                self._update_status_surtido_label()

                # Preguntar al usuario qué acción tomar
                msg_box = QtWidgets.QMessageBox(self)
                msg_box.setWindowTitle("Sin Registro Previo")
                msg_box.setText("No existe registro de mantenimiento para este herramental.")
                msg_box.setInformativeText("¿Qué acción deseas realizar?")
                msg_box.setIcon(QtWidgets.QMessageBox.Information)

                # Añadir botones personalizados con roles específicos
                btn_perform = msg_box.addButton("Realizar Mantenimiento Completo", QtWidgets.QMessageBox.AcceptRole)
                btn_edit = msg_box.addButton("Editar/Añadir Manualmente", QtWidgets.QMessageBox.ActionRole)
                btn_cancel = msg_box.addButton("Cancelar", QtWidgets.QMessageBox.RejectRole)

                # Aplicar estilos a los botones según el rol:
                # - Acept: fondo verde
                # - Danger: fondo amarillo
                # - Reject: fondo rojo
                btn_perform.setStyleSheet(BTN_STYLE_ACCEPT)
                btn_edit.setStyleSheet(BTN_STYLE_DANGER)
                btn_cancel.setStyleSheet(BTN_STYLE_REJECT)

                msg_box.setDefaultButton(btn_cancel)  # Botón por defecto
                msg_box.exec_()

                # Evaluar qué botón se presionó
                clicked_button = msg_box.clickedButton()
                if clicked_button == btn_perform:
                    print("Usuario eligió Realizar Mantenimiento Completo.")
                    self.perform_maintenance() # Llamar al proceso completo
                elif clicked_button == btn_edit:
                    print("Usuario eligió Editar/Añadir Manualmente.")
                    self.edit_maintenance() # Llamar al diálogo de edición
                else: # Cancelar o cerró la ventana
                    print("Usuario canceló la acción para registro N/A.")
                    # No hacer nada más, la UI ya muestra N/A

                return # Salir de update_info_display después de manejar el caso N/A

            else:
                # --- Caso: Hay registro, intentar procesar fecha ---
                try:
                    ultimo_date = datetime.strptime(str(ultimo_str), "%d/%m/%Y").date()
                    self.lblUltimoMantenimiento.setText(ultimo_date.strftime("%d/%m/%Y"))
                except ValueError:
                    # Error específico de formato de fecha
                    self.lblUltimoMantenimiento.setText(f"Formato Inválido ({ultimo_str})")
                    self.lblProximoMantenimiento.setText("Inválido")
                    status = "ERROR FECHA"
                    color = "lightcoral"
                    self.lblStatus.setText(status)
                    self.lblStatus.setStyleSheet(f"background-color: {color}; padding: 5px; border-radius: 3px;")
                    self._update_status_surtido_label() # Actualizar surtido igual
                    return # Salir si la fecha es inválida

        except Exception as e:
            # Capturar cualquier otro error inesperado al leer ULTIMO_MANTENIMIENTO
            print(f"Error inesperado al procesar ULTIMO_MANTENIMIENTO: {e}")
            QtWidgets.QMessageBox.critical(self, "Error Inesperado",
                                           f"Ocurrió un error al leer los datos de mantenimiento:\n{e}")
            self.lblUltimoMantenimiento.setText("Error")
            self.lblProximoMantenimiento.setText("Error")
            self.lblStatus.setText("ERROR")
            self.lblStatus.setStyleSheet("background-color: red; color: white; padding: 5px; border-radius: 3px;")
            self._update_status_surtido_label() # Actualizar surtido igual
            return # Salir en caso de error grave

        # --- Si llegamos aquí, ultimo_date es una fecha válida ---
        try:
            # Calcular Próximo Mantenimiento
            periodo = str(self.current_item.get("PERIODO", "Mensual")).lower() # Default razonable
            months_to_add = self.calculate_months_to_add(periodo) # Usar helper
            proximo_date_calculated = ultimo_date + relativedelta(months=months_to_add)
            self.lblProximoMantenimiento.setText(proximo_date_calculated.strftime("%d/%m/%Y"))

            # Calcular Status basado en Próximo Mantenimiento
            try:
                dias_alerta = pd.to_numeric(self.current_item.get("DIAS_ALERTA", 0), errors='coerce')
                dias_alerta = int(dias_alerta) if pd.notna(dias_alerta) else 0
            except (ValueError, TypeError):
                dias_alerta = 0

            current_date = datetime.today().date()
            status = self.calculate_status(current_date, proximo_date_calculated, dias_alerta) # Usar helper

            # Asignar color según el status calculado
            if status == "EN FECHA": color = "lightgreen"
            elif status == "MANTENIMIENTO CERCANO": color = "yellow"
            elif status == "VENCIDO": color = "lightcoral" # Rojo suave
            else: color = "lightgrey" # Color por defecto si el status es inesperado

            self.lblStatus.setText(status)
            self.lblStatus.setStyleSheet(f"background-color: {color}; padding: 5px; border-radius: 3px;")

        except Exception as e:
            # Capturar errores durante el cálculo de próximo/status
            print(f"Error al calcular Próximo Mantenimiento o Status: {e}")
            self.lblProximoMantenimiento.setText("Error Cálculo")
            self.lblStatus.setText("ERROR")
            self.lblStatus.setStyleSheet("background-color: red; color: white; padding: 5px; border-radius: 3px;")

        # --- Actualizar Status Surtido (siempre se ejecuta si no hubo return antes) ---
        self._update_status_surtido_label()


    # --- MÉTODOS HELPER ---

    def calculate_months_to_add(self, periodo_str):
        """Calcula cuántos meses añadir basado en el string del periodo."""
        periodo_lower = str(periodo_str).lower()
        if "bimestral" in periodo_lower: return 2
        if "trimestral" in periodo_lower: return 3
        if "semestral" in periodo_lower: return 6
        if "anual" in periodo_lower: return 12
        return 1 # Default a mensual

    def calculate_status(self, current_date, next_date_obj, alert_days):
        """
        Calcula el status comparando fechas.
        next_date_obj: Debe ser un objeto date o None.
        alert_days: Debe ser un int.
        """
        if not isinstance(next_date_obj, date): # Usar 'date' importado
             return "REQUIERE FECHA PRÓX."

        try:
             alert_days_int = int(alert_days)
             # Asegurar que días de alerta no sea negativo
             if alert_days_int < 0: alert_days_int = 0
             alert_date = next_date_obj - timedelta(days=alert_days_int)
        except (ValueError, TypeError):
             alert_date = next_date_obj # Fallback si dias_alerta es inválido
             print(f"Advertencia: Días de alerta '{alert_days}' inválido, usando 0.")

        if current_date < alert_date:
            return "EN FECHA"
        elif alert_date <= current_date <= next_date_obj:
            return "MANTENIMIENTO CERCANO"
        else: # current_date > next_date_obj
            return "VENCIDO"

    def _update_status_surtido_label(self):
        """Actualiza la etiqueta y estilo de 'Status Surtido'."""
        text_surtido = "N/A"
        color_surtido = "lightgrey" # Color neutro por defecto

        if self.current_item is not None:
            status_inout = self.current_item.get("STATUS_INOUT")
            text_surtido = "No Registrado" # Default si hay item pero no status
            color_surtido = "lightcoral"

            if pd.notna(status_inout):
                status_inout_lower = str(status_inout).strip().lower()
                if status_inout_lower == "in":
                    text_surtido, color_surtido = "Dentro", "lightgreen"
                elif status_inout_lower == "out":
                    text_surtido, color_surtido = "Surtido", "orange"

        self.lblStatusSurtido.setText(text_surtido)
        self.lblStatusSurtido.setStyleSheet(f"background-color: {color_surtido}; padding: 4px; border-radius: 3px; font-weight: bold;")


    def load_checklists_data(self):
        """Carga los datos del archivo JSON de checklists."""
        try:
            with open(CHECKLIST_PATH, 'r', encoding='utf-8-sig') as f:
                data = json.load(f)
                print(f"Checklist data cargada desde {CHECKLIST_PATH}")
                return data
        except FileNotFoundError:
            error_msg = f"No se encontró archivo de checklists:\n{CHECKLIST_PATH}"
            print(f"Error: {error_msg}")
            QtWidgets.QMessageBox.warning(self, "Archivo No Encontrado", error_msg)
            return {}
        except json.JSONDecodeError as e:
             error_msg = f"Archivo JSON de checklists corrupto:\n{CHECKLIST_PATH}\n\nError: {e}"
             print(f"Error: {error_msg}")
             QtWidgets.QMessageBox.critical(self, "Error Formato JSON", error_msg)
             return {}
        except Exception as e:
            error_msg = f"Error inesperado al cargar checklists:\n{e}"
            print(f"Error: {error_msg}")
            QtWidgets.QMessageBox.critical(self, "Error Carga Checklist", error_msg)
            return {}
           
    def refresh_data(self):
        self.updateCompleter()
        QtWidgets.QMessageBox.information(self, "Refresh", "Información actualizada.")

  
#Alerta    
    def update_alert_button_icon(self):
        alert_found = False
        try:
            df = pd.read_csv(self.filename, encoding='utf-8-sig')
            current_date = datetime.today().date()
            for index, row in df.iterrows():
                # OMITIR ítems con TYPE_INOUT "MULTIPLE" o TYPE_CONS_INOUT "CONSUMABLE"
                if (str(row.get("TYPE_INOUT", "")).strip().upper() == "MULTIPLE" or
                    str(row.get("TYPE_CONS_INOUT", "")).strip().upper() == "CONSUMABLE"):
                    continue

                ultimo = str(row.get("ULTIMO_MANTENIMIENTO", "")).strip()
                if not ultimo or ultimo.upper() == "N/A":
                    # Si no hay fecha de mantenimiento se marca alerta
                    alert_found = True
                    break

                try:
                    ultimo_date = datetime.strptime(ultimo, "%d/%m/%Y").date()
                except Exception:
                    alert_found = True
                    break

                periodo = str(row.get("PERIODO", "")).lower()
                months_to_add = 1
                if "bimestral" in periodo:
                    months_to_add = 2
                elif "trimestral" in periodo:
                    months_to_add = 3
                elif "semestral" in periodo:
                    months_to_add = 6
                elif "anual" in periodo:
                    months_to_add = 12

                proximo_date = ultimo_date + relativedelta(months=months_to_add)
                try:
                    dias_alerta = int(row.get("DIAS_ALERTA", 0))
                except:
                    dias_alerta = 0
                alerta_date = proximo_date - timedelta(days=dias_alerta)

                # Se detecta alerta si el mantenimiento está vencido o cercano.
                if current_date > proximo_date or (alerta_date <= current_date <= proximo_date):
                    alert_found = True
                    break
        except Exception as e:
            print("Error al actualizar el botón de alerta:", e)
        
        if alert_found:
            self.btn_alerta.setText("Alertas 🔴")
        else:
            self.btn_alerta.setText("Alertas")
    
    def show_alert_details(self):
        try:
            df = pd.read_csv(self.filename, encoding='utf-8-sig')
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", "No se pudo leer el CSV para mostrar alertas.")
            return

        alert_items = []
        current_date = datetime.today().date()
        for idx, row in df.iterrows():
            # Omitir ítems de tipo MULTIPLE/CONSUMABLE
            if (str(row.get("TYPE_INOUT", "")).strip().upper() == "MULTIPLE" or
                str(row.get("TYPE_CONS_INOUT", "")).strip().upper() == "CONSUMABLE"):
                continue

            nomen = str(row.get("NOMENCLATURA", "")).strip()
            ultimo = str(row.get("ULTIMO_MANTENIMIENTO", "")).strip()

            try:
                dias = int(row.get("DIAS_ALERTA", 0))
            except Exception:
                dias = 0

            if not ultimo or ultimo.upper() == "N/A":
                alert_items.append((nomen, "Sin mantenimiento"))
            else:
                try:
                    ultimo_date = datetime.strptime(ultimo, "%d/%m/%Y").date()
                    periodo = str(row.get("PERIODO", "")).lower()
                    months_to_add = 1
                    if "bimestral" in periodo:
                        months_to_add = 2
                    elif "trimestral" in periodo:
                        months_to_add = 3
                    elif "semestral" in periodo:
                        months_to_add = 6
                    elif "anual" in periodo:
                        months_to_add = 12

                    proximo_date = ultimo_date + relativedelta(months=months_to_add)
                    alerta_date = proximo_date - timedelta(days=dias)

                    if current_date > proximo_date:
                        alert_items.append((nomen, "Vencido"))
                    elif alerta_date <= current_date <= proximo_date:
                        alert_items.append((nomen, "Mantenimiento cercano"))
                except Exception:
                    alert_items.append((nomen, "Error en fecha"))

        if not alert_items:
            QtWidgets.QMessageBox.information(self, "Alertas", "No hay ítems en estado de alerta.")
            return

        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle("Alertas de Mantenimiento")
        dialog.resize(600, 300)
        layout = QtWidgets.QVBoxLayout(dialog)
        label = QtWidgets.QLabel("Seleccione un ítem para cargarlo en la búsqueda:")
        layout.addWidget(label)

        list_widget = QtWidgets.QListWidget()
        for item in alert_items:
            list_widget.addItem(f"{item[0]} - {item[1]}")
        layout.addWidget(list_widget)

        button_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Close)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)

        def on_item_double_clicked(item):
            texto = item.text()
            nomen_clatura = texto.split(" - ")[0].strip()
            self.search_field.setText(nomen_clatura)
            self.search_item()
            dialog.accept()

        list_widget.itemDoubleClicked.connect(on_item_double_clicked)
        dialog.exec_()
        
    def open_manage_checklists_dialog(self):
        dialog = ManageChecklistsDialog(DB_PATH, CHECKLIST_PATH, self)
        dialog.exec_()

    def perform_maintenance(self):
        """
        Realiza el proceso de mantenimiento para el ítem seleccionado,
        incluyendo la validación mediante un checklist dinámico y actualizando el CSV.
            """
        # Verificar que exista un ítem seleccionado
        if self.current_item is None:
            QtWidgets.QMessageBox.information(self, "Aviso", "No hay ningún elemento seleccionado para mantenimiento.")
            print("[DEBUG] No hay ítem seleccionado.")
            return

        # Asegurar que self.df esté definido: si no existe, cargarlo desde self.filename
        if not hasattr(self, 'df') or self.df is None:
            try:
                self.df = pd.read_csv(self.filename, encoding='utf-8-sig')
                print("[DEBUG] self.df cargado. Shape:", self.df.shape)
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo cargar el CSV:\n{e}")
                return
        nomenclatura = self.current_item.get('NOMENCLATURA', 'N/A')
        job = self.current_item.get('JOB', 'N/A')
        herramental_id_display = self.current_item.get('HERRAMENTAL_ID', 'N/A') # Para mostrar

        # --- PASO 1: Confirmación inicial ---
        msg_confirm = QtWidgets.QMessageBox(self)
        msg_confirm.setWindowTitle("Confirmar Mantenimiento")
        msg_confirm.setText(f"¿Seguro que quieres registrar el mantenimiento para:\n\n"
                            f"  ID: {herramental_id_display}\n"
                            f"  Nomenclatura: {nomenclatura}\n"
                            f"  Job: {job}")
        msg_confirm.setIcon(QtWidgets.QMessageBox.Question)
        btn_si_confirm = msg_confirm.addButton("Sí", QtWidgets.QMessageBox.YesRole)
        btn_no_confirm = msg_confirm.addButton("No", QtWidgets.QMessageBox.NoRole)
        # Estilo para botones (texto negro y negrita)
        button_style = "QPushButton { color: black; font-weight: bold; min-width: 60px; padding: 5px; }"
        btn_si_confirm.setStyleSheet(button_style)
        btn_no_confirm.setStyleSheet(button_style)
        msg_confirm.setDefaultButton(btn_no_confirm)
        msg_confirm.exec_()

        if msg_confirm.clickedButton() != btn_si_confirm:
            print("Mantenimiento cancelado por el usuario (confirmación inicial).")
            return

        # --- PASO 2: Revisión MPI (Obligatorio) ---
        msg_mpi = QtWidgets.QMessageBox(self)
        msg_mpi.setWindowTitle("Revisión MPI Obligatoria")
        msg_mpi.setText("Para continuar, debes revisar el documento MPI.\n"
                        "¿Deseas abrirlo ahora?")
        msg_mpi.setIcon(QtWidgets.QMessageBox.Question)
        btn_si_mpi = msg_mpi.addButton("Sí, abrir MPI", QtWidgets.QMessageBox.YesRole)
        btn_no_mpi = msg_mpi.addButton("No, cancelar", QtWidgets.QMessageBox.NoRole)
        btn_si_mpi.setStyleSheet(button_style)
        btn_no_mpi.setStyleSheet(button_style)
        msg_mpi.setDefaultButton(btn_no_mpi)
        msg_mpi.exec_()

        if msg_mpi.clickedButton() == btn_si_mpi:
            if not self.open_MPI_pdf(): # open_MPI_pdf debe retornar True/False
                # open_MPI_pdf ya mostró su propio mensaje de error/advertencia
                print("Fallo al abrir MPI detectado. Cancelando mantenimiento.")
                return # Detener si open_MPI_pdf indicó fallo
        else:
            QtWidgets.QMessageBox.information(self, "Mantenimiento Cancelado", "La revisión del documento MPI es obligatoria para continuar.")
            return

        # --- PASO 3: CHECKLIST DINÁMICO ---
        tipo_herramental_actual = str(self.current_item.get("TIPO DE HERRAMENTAL", "")).strip()
        if not tipo_herramental_actual:
             QtWidgets.QMessageBox.warning(self, "Falta Información",
                                           "El ítem seleccionado no tiene 'TIPO DE HERRAMENTAL' definido.\n"
                                           "No se puede mostrar checklist específico.\n"
                                           "El mantenimiento no puede continuar sin esta información.")
             return

        checklist_items = []
        checklist_display_title = tipo_herramental_actual # Título base
        try:
            checklists_data = self.load_checklists_data() # Usa la función helper
            if tipo_herramental_actual in checklists_data:
                 checklist_items = checklists_data[tipo_herramental_actual]
                 print(f"Checklist encontrado para tipo: {tipo_herramental_actual}")
            elif "DEFAULT" in checklists_data:
                 checklist_items = checklists_data["DEFAULT"]
                 checklist_display_title = f"{tipo_herramental_actual} (Usando Checklist DEFAULT)"
                 print("Checklist específico no encontrado, usando DEFAULT.")
            else:
                 print(f"No se encontró checklist específico para '{tipo_herramental_actual}' ni checklist DEFAULT.")
                 # El diálogo mostrará que no hay items

        except Exception as e:
            # load_checklists_data ya debería haber mostrado un error
            print(f"Excepción no manejada al procesar datos de checklist: {e}")
            # Considerar si detener o continuar sin checklist (actualmente continúa)
            # return

        # Mostrar diálogo de Checklist
        checklist_dialog = ChecklistDialog(checklist_display_title, checklist_items, self)
        if checklist_dialog.exec_() != QtWidgets.QDialog.Accepted:
             QtWidgets.QMessageBox.information(self, "Mantenimiento Cancelado", "Checklist no completado o cancelado.")
             return

        # --- PASO 4: Confirmación Final ---
        msg_final = QtWidgets.QMessageBox(self)
        msg_final.setWindowTitle("Confirmar Registro Final")
        msg_final.setText("Se han completado las verificaciones (MPI, Checklist).\n"
                        "¿Deseas registrar este mantenimiento en ToolTrack+?")
        msg_final.setIcon(QtWidgets.QMessageBox.Question)
        btn_si_final = msg_final.addButton("Sí, registrar", QtWidgets.QMessageBox.YesRole)
        btn_no_final = msg_final.addButton("No, cancelar", QtWidgets.QMessageBox.NoRole)
        btn_si_final.setStyleSheet(button_style)
        btn_no_final.setStyleSheet(button_style)
        msg_final.setDefaultButton(btn_no_final)
        msg_final.exec_()

        if msg_final.clickedButton() != btn_si_final:
            QtWidgets.QMessageBox.information(self, "Mantenimiento No Registrado", "Proceso cancelado antes de guardar cambios.")
            return

        # --- PASO 5: Actualizar Datos en CSV ---
        print("Procediendo a actualizar el archivo CSV...")
        try:
            # Leer el CSV original
            df_mod = pd.read_csv(self.filename, encoding='utf-8-sig')
            print(f"[DEBUG] CSV original leído. Shape: {df_mod.shape}")
        except FileNotFoundError:
            QtWidgets.QMessageBox.critical(self, "Error Fatal", f"No se encontró el archivo de base de datos:\n{self.filename}")
            return
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error Fatal", f"No se pudo leer el archivo CSV para actualizar:\n{e}")
            return

        # Encontrar el índice de la fila a actualizar (almacenado en la variable local idx)
        idx = None
        try:
            herramental_id = self.current_item.get("HERRAMENTAL_ID")
            if pd.notna(herramental_id):  # Buscar por ID
                df_mod["HERRAMENTAL_ID"] = pd.to_numeric(df_mod["HERRAMENTAL_ID"], errors='coerce')
                found_indices = df_mod[df_mod["HERRAMENTAL_ID"] == float(herramental_id)].index
                if not found_indices.empty:
                    idx = found_indices[0]

            if idx is None:  # Fallback a búsqueda por NOMENCLATURA
                search_value = str(self.current_item.get("NOMENCLATURA", "")).strip()
                if search_value and "NOMENCLATURA" in df_mod.columns:
                    found_indices = df_mod[df_mod["NOMENCLATURA"].astype(str).str.strip().str.upper() == search_value.upper()].index
                    if not found_indices.empty:
                        idx = found_indices[0]

            if idx is None:
                QtWidgets.QMessageBox.critical(self, "Error Fatal", "No se pudo encontrar el ítem en el CSV para actualizar.")
                return

            print(f"[DEBUG] Índice encontrado para actualizar: {idx}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Error al buscar índice: {e}")
            return

        # Aplicar los cambios en el DataFrame local (self.df)
        try:
            new_date = datetime.today().date()
            new_date_str = new_date.strftime("%d/%m/%Y")
            updates = {"ULTIMO_MANTENIMIENTO": new_date_str}

            # Calcular y añadir PROXIMO_MANTENIMIENTO
            periodo = str(df_mod.loc[idx, "PERIODO"]).lower() if pd.notna(df_mod.loc[idx, "PERIODO"]) else "mensual"
            months_to_add = self.calculate_months_to_add(periodo)
            nuevo_proximo = new_date + relativedelta(months=months_to_add)
            nuevo_proximo_str = nuevo_proximo.strftime("%d/%m/%Y")
            updates["PROXIMO_MANTENIMIENTO"] = nuevo_proximo_str

            # Limpiar y añadir DIAS_ALERTA
            try:
                dias_alerta = pd.to_numeric(df_mod.loc[idx, "DIAS_ALERTA"], errors='coerce')
                dias_alerta = int(dias_alerta) if pd.notna(dias_alerta) else 0
            except Exception:
                dias_alerta = 0
            updates["DIAS_ALERTA"] = dias_alerta

            # Calcular y añadir STATUS
            new_status_val = self.calculate_status(new_date, nuevo_proximo, dias_alerta)
            updates["STATUS"] = new_status_val

            # Añadir USER_LAST_MAINTENANCE, si la columna existe
            user_col = 'USER_LAST_MAINTENANCE'
            if user_col in df_mod.columns:
                try:
                    updates[user_col] = Session.user_alias
                except AttributeError:
                    print(f"Advertencia: No se pudo obtener Session.user_alias. Columna '{user_col}' no actualizada.")

            print(f"[DEBUG] Datos preparados para actualizar en la fila {idx}: {updates}")

            # Aplicar las actualizaciones al DataFrame local self.df. Usando idx en vez de self.current_item_idx.
            for col, value in updates.items():
                if col in self.df.columns:
                    self.df.loc[idx, col] = value
                else:
                    print(f"Advertencia: La columna '{col}' no existe en self.df. No se actualizará.")

            print(f"[DEBUG] self.df actualizado localmente. Fila {idx}:", self.df.loc[idx])
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error Procesamiento", f"Error al preparar datos para guardar:\n{e}")
            return

        # Usar FileLock para evitar conflictos en la escritura y realizar merge de datos
        try:
            from filelock import FileLock
            db_lock_path = self.filename + ".lock"
            lock = FileLock(db_lock_path, timeout=10)
            with lock:
                print("[DEBUG] Lock adquirido sobre el archivo CSV para actualización.")
                try:
                    current_df = pd.read_csv(self.filename, encoding='utf-8-sig')
                    print(f"[DEBUG] CSV re-leído para merge. Shape: {current_df.shape}")
                except Exception as e:
                    print("[DEBUG] Error al re-leer CSV:", e)
                    current_df = pd.DataFrame()

                if not current_df.empty and "NOMENCLATURA" in current_df.columns:
                    current_df = current_df.drop_duplicates(subset=["NOMENCLATURA"])
                    new_df = self.df.drop_duplicates(subset=["NOMENCLATURA"])
                    print(f"[DEBUG] current_df.shape tras drop_duplicates: {current_df.shape}")
                    print(f"[DEBUG] new_df.shape tras drop_duplicates: {new_df.shape}")

                    current_df.set_index("NOMENCLATURA", inplace=True)
                    new_df.set_index("NOMENCLATURA", inplace=True)

                    common_cols = current_df.columns.intersection(new_df.columns)
                    for col in common_cols:
                        if pd.api.types.is_numeric_dtype(current_df[col]) and new_df[col].dtype == object:
                            new_df[col] = pd.to_numeric(new_df[col], errors="coerce")

                    current_df.update(new_df)
                    new_rows = new_df.loc[~new_df.index.isin(current_df.index)]
                    merged_df = pd.concat([current_df, new_rows])
                    merged_df.reset_index(inplace=True)
                    updated_df = merged_df.copy()
                    print(f"[DEBUG] Merge realizado. Resultado shape: {updated_df.shape}")
                else:
                    updated_df = self.df.copy()
                    print("[DEBUG] CSV sin 'NOMENCLATURA', se usa self.df directamente.")

                updated_df.to_csv(self.filename, index=False, encoding='utf-8-sig')
                print(f"[DEBUG] Archivo CSV '{self.filename}' actualizado correctamente.")
                self.df = updated_df.copy()  # Actualizar self.df con el contenido final
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error Guardado", f"No se pudo guardar el archivo CSV:\n{e}")
            print("[DEBUG] Excepción al guardar CSV:", e)
            return

        QtWidgets.QMessageBox.information(self, "Éxito", f"Mantenimiento para '{nomenclatura}' registrado correctamente.")
        print(f"[DEBUG] Proceso de mantenimiento completado para {nomenclatura}.")

        # --- PASO 6: Finalización y Actualización UI ---
        QtWidgets.QMessageBox.information(self, "Éxito", f"Mantenimiento para '{nomenclatura}' registrado correctamente.")

        # Actualizar la UI con los datos guardados
        self.current_item = df_mod.loc[idx].copy()  # Cargar la fila actualizada desde el CSV original (previo al merge)
        self.update_info_display()
        self.update_alert_button_icon()

        try:
            write_history(Session.user_alias, "Mantenimiento Realizado", nomenclatura)
            print("Acción registrada en el historial.")
        except Exception as e:
            print(f"Advertencia: Error al escribir en el historial: {e}")

        print(f"Proceso de mantenimiento completado para {nomenclatura}.")

# --- MÉTODO edit_maintenance ACTUALIZADO, CORREGIDO Y OPTIMIZADO ---
    def edit_maintenance(self):
        """
        Permite editar manualmente los datos base del mantenimiento (último, periodo, días)
        y actualiza el usuario que realizó la edición, recalculando próximo/status.
        Se utiliza FileLock para adquirir un lock exclusivo sobre el archivo antes de leerlo
        nuevamente y realizar la fusión (merge) de los datos modificados con el CSV.
        """
        if self.current_item is None:
            QtWidgets.QMessageBox.information(self, "Aviso", "No hay ningún ITEM seleccionado para editar.")
            return

        # --- Validar sesión ya iniciada y permisos de edición ---
        if not Session.user_alias:
            QtWidgets.QMessageBox.critical(self, "Error de sesión",
                                        "La sesión aún no está iniciada. Reinicia la aplicación e inicia sesión.")
            return

        if not check_update_permission(Session.user_alias):
            QtWidgets.QMessageBox.warning(self, "Permisos insuficientes",
                                        "No cuentas con los permisos para editar.")
            return

        print(f"[DEBUG] Permisos de edición concedidos a {Session.user_alias}")

        # --- Abrir Diálogo de Edición ---
        edit_dialog = EditMaintenanceDialog(self.current_item, self)  # Asume que esta clase existe
        if edit_dialog.exec_() == QtWidgets.QDialog.Accepted:
            new_data = edit_dialog.get_data()  # Obtiene {'ULTIMO_MANTENIMIENTO': ..., 'PERIODO': ..., 'DIAS_ALERTA': ...}

            # --- Preparar Datos y Leer CSV ---
            print("[DEBUG] Procesando datos de edición...")
            try:
                # Validar y limpiar datos del diálogo
                ultimo_mantenimiento_str = new_data.get("ULTIMO_MANTENIMIENTO", "").strip()
                if not ultimo_mantenimiento_str or ultimo_mantenimiento_str.upper() == "N/A":
                    ultimo_mantenimiento_str = "N/A"

                periodo_str = new_data.get("PERIODO", "Mensual")  # Default razonable

                try:
                    dias_alerta_val = new_data.get("DIAS_ALERTA", "0")
                    # Intentar convertir a float y luego a int para asegurar número entero
                    dias_alerta_int = int(float(dias_alerta_val if dias_alerta_val else "0"))
                except (ValueError, TypeError):
                    QtWidgets.QMessageBox.warning(self, "Dato Inválido", "Valor para 'Días Alerta' no válido. Se usará 0.")
                    dias_alerta_int = 0

                # Leer el DataFrame del CSV
                df = pd.read_csv(self.filename, encoding='utf-8-sig')
                print(f"[DEBUG] CSV leído para edición. Shape: {df.shape}")
            except FileNotFoundError:
                QtWidgets.QMessageBox.critical(self, "Error Fatal", f"No se encontró el archivo:\n{self.filename}")
                return
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error Preparación", f"Error al preparar datos o leer CSV para editar:\n{e}")
                return

            # --- Encontrar el índice del ítem ---
            idx = None
            try:
                herramental_id = self.current_item.get("HERRAMENTAL_ID")
                if pd.notna(herramental_id):  # Priorizar búsqueda por ID
                    df["HERRAMENTAL_ID"] = pd.to_numeric(df["HERRAMENTAL_ID"], errors='coerce')
                    found_indices = df[df["HERRAMENTAL_ID"] == float(herramental_id)].index
                    if not found_indices.empty:
                        idx = found_indices[0]

                if idx is None:  # Fallback a búsqueda por NOMENCLATURA
                    nomenclatura_actual = str(self.current_item.get("NOMENCLATURA", "")).strip()
                    if nomenclatura_actual and "NOMENCLATURA" in df.columns:
                        found_indices = df[df["NOMENCLATURA"].astype(str).str.strip().str.upper() == nomenclatura_actual.upper()].index
                        if not found_indices.empty:
                            idx = found_indices[0]

                if idx is None:
                    QtWidgets.QMessageBox.critical(self, "Error Fatal", "No se pudo encontrar el ítem en CSV para editar.")
                    return

                print(f"[DEBUG] Índice encontrado para actualizar: {idx}")
            except KeyError as e:
                QtWidgets.QMessageBox.critical(self, "Error Fatal", f"Falta columna {e} en CSV.")
                return
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error", f"Error al buscar índice para editar: {e}")
                return

            # --- Aplicar cambios y Recalcular ---
            updates = {}  # Diccionario para acumular cambios
            try:
                updates["ULTIMO_MANTENIMIENTO"] = ultimo_mantenimiento_str
                updates["PERIODO"] = periodo_str
                updates["DIAS_ALERTA"] = dias_alerta_int

                # Añadir usuario que edita
                user_col = 'USER_LAST_MAINTENANCE'
                if user_col in df.columns:
                    try:
                        updates[user_col] = Session.user_alias
                        print(f"[DEBUG] Registrando edición por usuario: {Session.user_alias}")
                    except AttributeError:
                        print(f"[DEBUG] Advertencia: No se pudo obtener Session.user_alias. Columna '{user_col}' no actualizada en edición.")

                # Recalcular Próximo Mantenimiento y Status si la fecha es válida
                if ultimo_mantenimiento_str != "N/A":
                    try:
                        ultimo_date_edit = datetime.strptime(ultimo_mantenimiento_str, "%d/%m/%Y").date()
                        months_to_add_edit = self.calculate_months_to_add(periodo_str.lower())
                        proximo_date_edit = ultimo_date_edit + relativedelta(months=months_to_add_edit)
                        proximo_date_edit_str = proximo_date_edit.strftime("%d/%m/%Y")
                        updates["PROXIMO_MANTENIMIENTO"] = proximo_date_edit_str

                        # Recalcular status también
                        status_edit = self.calculate_status(datetime.today().date(), proximo_date_edit, dias_alerta_int)
                        updates["STATUS"] = status_edit
                        print("[DEBUG] Próximo Mantenimiento y Status recalculados.")
                    except ValueError:
                        print(f"[DEBUG] Fecha '{ultimo_mantenimiento_str}' inválida. No se recalcula Próximo/Status.")
                        updates["PROXIMO_MANTENIMIENTO"] = "N/A"
                        updates["STATUS"] = "ERROR FECHA"
                    except Exception as e:
                        print(f"[DEBUG] Error inesperado al recalcular Próximo/Status en edición: {e}")
                        updates["PROXIMO_MANTENIMIENTO"] = "Error"
                        updates["STATUS"] = "Error"
                else:
                    updates["PROXIMO_MANTENIMIENTO"] = "N/A"
                    updates["STATUS"] = "REQUIERE REGISTRO"
                    print("[DEBUG] Fecha es N/A. Próximo Mantenimiento y Status limpiados.")

                # Aplicar todas las actualizaciones al DataFrame
                for col, value in updates.items():
                    if col in df.columns:
                        df.loc[idx, col] = value
                    else:
                        print(f"[DEBUG] Advertencia: La columna '{col}' no existe en el CSV. No se pudo actualizar.")

                print(f"[DEBUG] Datos de edición preparados para guardar en el índice {idx}: {updates}")
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error Procesamiento", f"Error al procesar datos editados:\n{e}")
                return

            # --- Guardar cambios en el CSV ---
            try:
                from filelock import FileLock
                db_lock_path = self.filename + ".lock"
                lock = FileLock(db_lock_path, timeout=10)
                with lock:
                    print("[DEBUG] Lock adquirido para actualizar CSV en edición.")
                    try:
                        current_df = pd.read_csv(self.filename, encoding='utf-8-sig')
                        print(f"[DEBUG] CSV re-leído para merge en edición. Shape: {current_df.shape}")
                    except Exception as e:
                        print("[DEBUG] Error al re-leer CSV en edición:", e)
                        current_df = pd.DataFrame()

                    if not current_df.empty and "NOMENCLATURA" in current_df.columns:
                        current_df = current_df.drop_duplicates(subset=["NOMENCLATURA"])
                        new_df = df.drop_duplicates(subset=["NOMENCLATURA"])
                        print(f"[DEBUG] current_df.shape tras drop_duplicates: {current_df.shape}")
                        print(f"[DEBUG] new_df.shape tras drop_duplicates: {new_df.shape}")

                        current_df.set_index("NOMENCLATURA", inplace=True)
                        new_df.set_index("NOMENCLATURA", inplace=True)

                        common_cols = current_df.columns.intersection(new_df.columns)
                        for col in common_cols:
                            if pd.api.types.is_numeric_dtype(current_df[col]) and new_df[col].dtype == object:
                                new_df[col] = pd.to_numeric(new_df[col], errors="coerce")

                        current_df.update(new_df)
                        new_rows = new_df.loc[~new_df.index.isin(current_df.index)]
                        merged_df = pd.concat([current_df, new_rows])
                        merged_df.reset_index(inplace=True)
                        updated_df = merged_df.copy()
                        print(f"[DEBUG] Merge realizado en edición. Resultado shape: {updated_df.shape}")
                    else:
                        updated_df = df.copy()
                        print("[DEBUG] CSV sin 'NOMENCLATURA' en edición, se usa df directamente.")

                    updated_df.to_csv(self.filename, index=False, encoding='utf-8-sig')
                    print(f"[DEBUG] CSV '{self.filename}' actualizado correctamente desde edición.")
                    # Actualizar el DataFrame local con el contenido final
                    df = updated_df.copy()
                QtWidgets.QMessageBox.information(self, "Éxito", "Los datos de mantenimiento se han actualizado.")
                self.current_item = df.loc[idx].copy()
                self.update_info_display()
                self.update_alert_button_icon()

                try:
                    write_history(Session.user_alias, "Edicion de mantenimiento", self.current_item.get("NOMENCLATURA", "N/A"))
                    print("[DEBUG] Edición registrada en el historial.")
                except Exception as e:
                    print(f"[DEBUG] Advertencia: Error al escribir edición en historial: {e}")

            except PermissionError:
                QtWidgets.QMessageBox.critical(self, "Error Guardado", f"No se pudo guardar:\n{self.filename}\n\nArchivo abierto o sin permisos.")
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error Guardado", f"No se pudo guardar el CSV tras editar:\n{e}")

        else:
            print("[DEBUG] Edición de mantenimiento cancelada por el usuario.")

    # --- NUEVA FUNCIÓN HELPER para cargar checklists ---
    def load_checklists_data(self):
        """Carga los datos del archivo JSON de checklists."""
        try:
            # Asegúrate que CHECKLIST_PATH esté definido correctamente.
            # Puede ser una constante global o un atributo self.checklist_path si lo prefieres.
            # Ejemplo usando una constante global (asegúrate que esté definida arriba):
            # CHECKLIST_PATH = r"\\gdlnt104\ScanDirs\B18\ToolTrack+\Recursos\checklists.json"

            with open(CHECKLIST_PATH, 'r', encoding='utf-8-sig') as f:
                data = json.load(f)
                print(f"Checklist data loaded successfully from {CHECKLIST_PATH}")
                return data
        except FileNotFoundError:
            error_msg = f"No se encontró el archivo de checklists:\n{CHECKLIST_PATH}\nEl checklist no estará disponible."
            print(f"Error: {error_msg}")
            QtWidgets.QMessageBox.warning(self, "Archivo Checklist No Encontrado", error_msg)
            # Devolver diccionario vacío para que el resto del código no falle,
            # aunque el checklist no funcione.
            return {}
        except json.JSONDecodeError as e:
             error_msg = f"El archivo JSON de checklists está corrupto o mal formado:\n{CHECKLIST_PATH}\n\nError: {e}"
             print(f"Error: {error_msg}")
             QtWidgets.QMessageBox.critical(self, "Error Formato JSON", error_msg)
             return {} # Devolver vacío para evitar más errores
        except Exception as e:
            error_msg = f"Error inesperado al cargar checklists desde {CHECKLIST_PATH}:\n{e}"
            print(f"Error: {error_msg}")
            QtWidgets.QMessageBox.critical(self, "Error Carga Checklist", error_msg)
            return {} # Devolver vacío

    # --- Tus otros métodos como perform_maintenance, open_MPI_pdf, etc. ---
    # ... (asegúrate que la indentación sea consistente) ...
    #     
# --- MÉTODO open_MPI_pdf CORREGIDO ---
    def open_MPI_pdf(self):
        """
        Busca y abre el archivo PDF asociado al MPI del ítem actual.
        Retorna True si el archivo o URL se intentó abrir con éxito, False en caso contrario.
        """
        if self.current_item is None:
            QtWidgets.QMessageBox.information(self, "Aviso", "No hay ningún ítem seleccionado.")
            return False  # Indicar fallo

        pdf_path = None
        herramental_id = self.current_item.get("HERRAMENTAL_ID")

        try:
            # Leer el CSV para buscar la ruta del MPI
            df = pd.read_csv(self.filename, encoding='utf-8-sig')

            # Determinar el filtro (ID o Nomenclatura)
            if pd.notna(herramental_id):
                df["HERRAMENTAL_ID"] = pd.to_numeric(df["HERRAMENTAL_ID"], errors='coerce')
                filtro = df["HERRAMENTAL_ID"] == float(herramental_id)
            else:
                item_key = str(self.current_item.get("NOMENCLATURA", "")).strip()
                if not item_key:
                    QtWidgets.QMessageBox.warning(self, "Aviso", "El ítem actual no tiene NOMENCLATURA ni HERRAMENTAL_ID para buscar el MPI.")
                    return False  # Indicar fallo
                # Asegurar que la columna existe antes de filtrar
                if "NOMENCLATURA" not in df.columns:
                    QtWidgets.QMessageBox.critical(self, "Error Estructura", "La columna 'NOMENCLATURA' no existe en el CSV.")
                    return False  # Indicar fallo
                filtro = df["NOMENCLATURA"].astype(str).str.strip().str.upper() == item_key.upper()

            df_result = df[filtro]

            if df_result.empty:
                QtWidgets.QMessageBox.warning(self, "No Encontrado", "No se encontró el registro del ítem actual en el CSV para obtener la ruta MPI.")
                return False  # Indicar fallo

            # Asegurarse de que la columna MPI existe
            if "MPI" not in df_result.columns:
                QtWidgets.QMessageBox.warning(self, "Falta Columna", "La columna 'MPI' no existe en el archivo CSV.")
                return False  # Indicar fallo

            # Obtener la ruta del PDF de la primera fila encontrada
            pdf_path = df_result.iloc[0]["MPI"]

        except FileNotFoundError:
            QtWidgets.QMessageBox.critical(self, "Error Archivo", f"No se encontró el archivo CSV:\n{self.filename}")
            return False  # Indicar fallo
        except KeyError as e:
            QtWidgets.QMessageBox.critical(self, "Error Columna", f"Falta una columna esperada ('HERRAMENTAL_ID' o 'NOMENCLATURA') en el CSV: {e}")
            return False  # Indicar fallo
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error Lectura CSV", f"No se pudo leer o procesar el CSV para buscar MPI:\n{e}")
            return False  # Indicar fallo

        # --- Validar la ruta o URL del PDF ---
        if pd.isnull(pdf_path) or not isinstance(pdf_path, str) or not pdf_path.strip():
            QtWidgets.QMessageBox.warning(self, "MPI No Definido", "La ruta del archivo MPI no está definida (vacía o no es texto) para este ítem.")
            return False  # Indicar fallo

        pdf_path = pdf_path.strip()  # Eliminar espacios en blanco

        # Verificar si se trata de una URL (no es PDF local)
        if pdf_path.lower().startswith("http://") or pdf_path.lower().startswith("https://"):
            try:
                print(f"Abriendo URL MPI en: {pdf_path}")
                webbrowser.open(pdf_path)
                return True  # Éxito al abrir la URL
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error al Abrir URL", f"No se pudo abrir la URL MPI:\n{pdf_path}\n\nError: {e}")
                return False  # Error al abrir URL
        else:
            # Verificar si el archivo existe en el sistema local
            if not os.path.isfile(pdf_path):
                QtWidgets.QMessageBox.warning(self, "Archivo No Encontrado", f"No se encontró el archivo MPI en la ruta especificada:\n{pdf_path}")
                return False  # Indicar fallo

            # --- Intentar abrir el archivo local ---
            try:
                print(f"Intentando abrir MPI en: {pdf_path}")
                os.startfile(pdf_path)
                print("Llamada a os.startfile realizada.")
                return True  # Éxito al abrir el archivo
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error al Abrir PDF", f"No se pudo abrir el archivo PDF:\n{pdf_path}\n\nError: {e}")
                return False  # Error al abrir el archivo
        
# Diálogo personalizado para editar datos del item en una sola ventana.
class EditItemDialog(QtWidgets.QDialog):
    def __init__(self, current_item, parent=None):
        super(EditItemDialog, self).__init__(parent)
        self.setWindowTitle("Modificar datos del item")
        self.resize(900, 300)
        self.fields = [
            "ITEM_TYPE", "NOMENCLATURA", "MODELO", "JOB",
            "TIPO DE HERRAMENTAL", "PROCESO", "PROYECTO", "RACK"
        ]
        self.edits = {}
        layout = QtWidgets.QFormLayout(self)
        for field in self.fields:
            edit = QtWidgets.QLineEdit(self)
            # Se carga el valor actual del campo.
            edit.setText(str(current_item.get(field, "")))
            layout.addRow(field, edit)
            self.edits[field] = edit

        # Botones Aceptar/Cancelar.
        button_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def getData(self):
        # Devuelve un diccionario con los nuevos valores.
        data = {}
        for field in self.fields:
            data[field] = self.edits[field].text()
        return data
            
# --- Diálogo Auxiliar para Selección de Ítem (cuando hay múltiples coincidencias) ---
class ItemSelectionDialog(QtWidgets.QDialog):
    def __init__(self, items, parent=None):
        """
        items: lista de diccionarios que deben incluir las claves "DF_IDX", 
               "NOMENCLATURA" y "JOB".
        """
        super().__init__(parent)
        self.setWindowTitle("Seleccione un Ítem")
        self.resize(900, 300)
        self.selected_df_index = None
        layout = QtWidgets.QVBoxLayout(self)
        
        self.table = QtWidgets.QTableWidget(self)
        self.table.setColumnCount(2)
        self.table.setHorizontalHeaderLabels(["Nomenclatura", "JOB"])
        self.table.setRowCount(len(items))
        self.items = items  # Lista de ítems
        for row, item in enumerate(items):
            nomenItem = QtWidgets.QTableWidgetItem(str(item.get("NOMENCLATURA", "")))
            jobItem = QtWidgets.QTableWidgetItem(str(item.get("JOB", "")))
            self.table.setItem(row, 0, nomenItem)
            self.table.setItem(row, 1, jobItem)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        
        # Establece que cada columna se expanda para ocupar todo el ancho de la tabla.
        self.table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        
        layout.addWidget(self.table)
        
        buttons = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.on_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
    
    def on_accept(self):
        selected = self.table.currentRow()
        if selected < 0:
            QtWidgets.QMessageBox.warning(self, "Advertencia", "Seleccione un ítem de la lista.")
            return
        self.selected_df_index = self.items[selected].get("DF_IDX")
        self.accept()

# --- Diálogo para registrar Estado y Comentario ---
class EstadoComentarioDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Registrar Estado y Comentario")
        self.resize(400, 200)
        
        layout = QtWidgets.QFormLayout(self)
        layout.setSpacing(10)
        
        self.estado_combo = QtWidgets.QComboBox()
        self.estado_combo.addItems(["LIMPIO / BUENO", "SUCIO / PARA LAVAR", "DAÑADO"])
        layout.addRow("Estado:", self.estado_combo)
        
        self.comentario_edit = QtWidgets.QLineEdit()
        layout.addRow("Comentario:", self.comentario_edit)
        
        # Botones de acción
        button_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addRow(button_box)
    
    def get_data(self):
        return self.estado_combo.currentText(), self.comentario_edit.text().strip()

# --- Clase Window5Page ---
class Window5Page(QtWidgets.QWidget):
    def __init__(self, user_alias, parent=None):
        super().__init__(parent)
        self.user_alias = user_alias
        self.current_item = None
        self.current_item_idx = None
        self.df = pd.DataFrame()
        self.initUI()
        self.load_csv_data()

    def initUI(self):
        # Layout principal con separación y márgenes
        self.layout = QtWidgets.QVBoxLayout(self)
        self.layout.setSpacing(15)
        self.layout.setContentsMargins(15, 15, 15, 15)

        # --- Contenedor de Búsqueda ---
        search_container = QtWidgets.QWidget()
        search_layout = QtWidgets.QHBoxLayout(search_container)
        search_layout.setSpacing(5)
        search_layout.setContentsMargins(0, 0, 0, 0)
        self.search_field = QtWidgets.QLineEdit()
        self.search_field.setPlaceholderText("Buscar por nomenclatura o JOB")
        self.search_field.setStyleSheet("""
            QLineEdit {
                border: 1px solid #d99227;
                border-radius: 5px;
                padding: 5px;
                font: 10pt 'Montserrat';
            }
        """)
        # Conectar el evento de presionar 'Enter' a la búsqueda
        self.search_field.returnPressed.connect(self.search_item)
        search_layout.addWidget(self.search_field)
        self.search_button = QtWidgets.QPushButton("Buscar")
        self.search_button.setStyleSheet(STYLE_BUTTON)
        self.search_button.clicked.connect(self.search_item)
        search_layout.addWidget(self.search_button)
        self.refresh_button = QtWidgets.QPushButton("Refresh")
        self.refresh_button.setStyleSheet(STYLE_BUTTON)
        self.refresh_button.clicked.connect(self.load_csv_data)
        search_layout.addWidget(self.refresh_button)
        self.layout.addWidget(search_container)
        
        # Configuración del Completer: se usa MatchContains para autocompletar con substring
        self.completer = QtWidgets.QCompleter()
        self.completer.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
        self.completer.setFilterMode(QtCore.Qt.MatchContains)
        self.search_field.setCompleter(self.completer)
        
        # --- Panel de Detalles desglosado individualmente ---
        # Usamos un QFormLayout para mostrar cada campo en una fila independiente.
        self.details_layout = QtWidgets.QFormLayout()
        self.details_layout.setSpacing(10)

        # Definir fuente para el panel de detalles (en este ejemplo, Montserrat 14pt)
        font_details = QtGui.QFont("Montserrat", 14)

        # Datos del Ítem desglosados: Nomenclatura, JOB y RACK
        self.nomen_label = QtWidgets.QLabel("")
        self.nomen_label.setFont(font_details)
        self.job_label = QtWidgets.QLabel("")
        self.job_label.setFont(font_details)
        self.rack_label = QtWidgets.QLabel("")
        self.rack_label.setFont(font_details)
        label_nomen = QtWidgets.QLabel("Nomenclatura:")
        label_nomen.setFont(font_details)
        label_job = QtWidgets.QLabel("JOB:")
        label_job.setFont(font_details)
        label_rack = QtWidgets.QLabel("RACK:")
        label_rack.setFont(font_details)
        self.details_layout.addRow(label_nomen, self.nomen_label)
        self.details_layout.addRow(label_job, self.job_label)
        self.details_layout.addRow(label_rack, self.rack_label)

        # Estado
        self.status_label = QtWidgets.QLabel("Estado: ---")
        self.status_label.setFont(font_details)
        label_estado = QtWidgets.QLabel("Estado:")
        label_estado.setFont(font_details)
        self.details_layout.addRow(label_estado, self.status_label)

        # Última salida
        self.last_out_label = QtWidgets.QLabel("Última salida: ---")
        self.last_out_label.setFont(font_details)
        label_last_out = QtWidgets.QLabel("Última salida:")
        label_last_out.setFont(font_details)
        self.details_layout.addRow(label_last_out, self.last_out_label)

        # Surtido a (visible solo si corresponde)
        self.user_out_label = QtWidgets.QLabel("")
        self.user_out_label.setFont(font_details)
        label_surtido_a = QtWidgets.QLabel("Surtido a:")
        label_surtido_a.setFont(font_details)
        self.details_layout.addRow(label_surtido_a, self.user_out_label)
        self.user_out_label.setVisible(False)

        # Surtido por (visible solo si corresponde)
        self.employee_out_label = QtWidgets.QLabel("")
        self.employee_out_label.setFont(font_details)
        label_surtido_por = QtWidgets.QLabel("Surtido por:")
        label_surtido_por.setFont(font_details)
        self.details_layout.addRow(label_surtido_por, self.employee_out_label)
        self.employee_out_label.setVisible(False)

        # Stock (visible solo en caso de ser aplicable)
        self.lblMultiStock = QtWidgets.QLabel("")
        self.lblMultiStock.setFont(font_details)
        label_stock = QtWidgets.QLabel("Stock:")
        label_stock.setFont(font_details)
        self.details_layout.addRow(label_stock, self.lblMultiStock)
        self.lblMultiStock.setVisible(False)

        # Se agrega el layout de detalles al layout principal
        self.layout.addLayout(self.details_layout)
        
        # --- Botones de Acción - Prioritarios ---
        top_btn_container = QtWidgets.QWidget()
        top_btn_layout = QtWidgets.QHBoxLayout(top_btn_container)
        top_btn_layout.setSpacing(15)
        top_btn_layout.setContentsMargins(0, 0, 0, 0)
        self.surtir_button = QtWidgets.QPushButton("SURTIR")
        self.surtir_button.setFixedHeight(50)
        self.surtir_button.setMinimumWidth(150)
        self.surtir_button.setStyleSheet(STYLE_BUTTON + "font-size: 14pt;")
        self.surtir_button.clicked.connect(self.surtir_action)
        top_btn_layout.addWidget(self.surtir_button)
        self.ingresar_button = QtWidgets.QPushButton("INGRESAR")
        self.ingresar_button.setFixedHeight(50)
        self.ingresar_button.setMinimumWidth(150)
        self.ingresar_button.setStyleSheet(STYLE_BUTTON + "font-size: 14pt;")
        self.ingresar_button.clicked.connect(self.ingresar_action)
        top_btn_layout.addWidget(self.ingresar_button)
        self.layout.addWidget(top_btn_container, alignment=QtCore.Qt.AlignCenter)
                # Botón para el flujo de limpieza (visible solo cuando el estado es "limpieza")
        self.limpiar_button = QtWidgets.QPushButton("LIMPIAR")
        self.limpiar_button.setFixedHeight(50)
        self.limpiar_button.setMinimumWidth(150)
        self.limpiar_button.setStyleSheet(STYLE_BUTTON + "font-size: 14pt;")
        self.limpiar_button.clicked.connect(self.limpiar_action)
        top_btn_layout.addWidget(self.limpiar_button)
        self.limpiar_button.hide()
        self.layout.addWidget(top_btn_container, alignment=QtCore.Qt.AlignCenter)

        # --- Botones de Acción - Secundarios ---
        bottom_btn_container = QtWidgets.QWidget()
        bottom_btn_layout = QtWidgets.QHBoxLayout(bottom_btn_container)
        bottom_btn_layout.setSpacing(10)
        bottom_btn_layout.setContentsMargins(0, 0, 0, 0)
        self.modificar_button = QtWidgets.QPushButton("MODIFICAR")
        self.modificar_button.setStyleSheet(STYLE_BUTTON)
        self.modificar_button.clicked.connect(self.modificar_action)
        bottom_btn_layout.addWidget(self.modificar_button)
        self.layout.addWidget(bottom_btn_container)
        
    def update_completer_model(self):
        # Filtra el DataFrame para excluir los items cuyo "TIPO DE HERRAMENTAL" sea "CONSUMABLE"
        filtered_df = self.df[
            self.df["TIPO DE HERRAMENTAL"].str.strip().str.upper() != "CONSUMABLE"
        ]
        
        # Extrae los valores únicos de las columnas a usar para autocompletar
        # Extraemos "NOMENCLATURA", "TIPO DE HERRAMENTAL" y "JOB"
        nomen_list = []
        if "NOMENCLATURA" in filtered_df.columns:
            nomen_list = filtered_df["NOMENCLATURA"].dropna().unique().tolist()
            nomen_list = [n.strip() for n in nomen_list]
        
        tipo_list = []
        if "TIPO DE HERRAMENTAL" in filtered_df.columns:
            tipo_list = filtered_df["TIPO DE HERRAMENTAL"].dropna().unique().tolist()
            tipo_list = [t.strip() for t in tipo_list]  # Aunque aquí ya se filtraron los "CONSUMABLE"
        
        job_list = []
        if "JOB" in filtered_df.columns:
            job_list = filtered_df["JOB"].dropna().unique().tolist()
            job_list = [j.strip() for j in job_list]
        
        # Une las listas y elimina duplicados
        union_list = list(set(nomen_list + tipo_list + job_list))
        union_list.sort()  # Opcional: para ordenar alfabéticamente
        
        # Crea un modelo de lista y asígnalo al QCompleter
        completer_model = QtCore.QStringListModel(union_list)
        self.completer.setModel(completer_model)


    def load_csv_data(self):
        try:
            self.df = pd.read_csv(DB_PATH, encoding='utf-8-sig', 
                                dtype={'USER_OUT': str, 'EMPLOYEE_OUT': str})
            
            # Si la columna NOMENCLATURA no existe, se crea vacía
            if "NOMENCLATURA" not in self.df.columns:
                self.df["NOMENCLATURA"] = ""
            
            # Verificar y crear (si es necesario) la columna USER_OUT
            if "USER_OUT" not in self.df.columns:
                self.df["USER_OUT"] = ""
            else:
                self.df["USER_OUT"] = self.df["USER_OUT"].fillna("").astype(str)
            
            # Verificar y crear (si es necesario) la columna EMPLOYEE_OUT
            if "EMPLOYEE_OUT" not in self.df.columns:
                self.df["EMPLOYEE_OUT"] = ""
            else:
                self.df["EMPLOYEE_OUT"] = self.df["EMPLOYEE_OUT"].fillna("").astype(str)
            
            # Actualiza el modelo del autocompletado partiendo del DataFrame filtrado
            self.update_completer_model()
            
            # --- Resetear la búsqueda y limpiar el display de resultados ---
            self.search_field.clear()  # Limpia el campo de búsqueda
            # Limpiar los labels de detalles
            self.nomen_label.clear()
            self.job_label.clear()
            self.rack_label.clear()
            self.status_label.clear()
            self.status_label.setStyleSheet("")  # Resetea el estilo
            self.last_out_label.clear()
            self.last_out_label.setStyleSheet("")  # Resetea el estilo
            self.user_out_label.clear()
            self.employee_out_label.clear()
            self.lblMultiStock.clear()
        
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", 
                                        f"No se pudo cargar el archivo CSV:\n{e}")
            self.df = pd.DataFrame()


    def search_item(self):
        # 1. Guardar el texto de búsqueda actual ANTES de que load_csv_data lo borre.
        query_text = self.search_field.text().strip()

        # 2. Recargar los datos desde el CSV.
        # Esto actualizará self.df con los datos más recientes y también
        # limpiará los campos de la UI (incluyendo self.search_field)
        # según la implementación actual de load_csv_data.
        self.load_csv_data()

        # 3. Verificar si el texto de búsqueda original (antes de limpiar el campo) estaba vacío.
        if not query_text: # Equivalente a query_text == ""
            QtWidgets.QMessageBox.warning(self, "Advertencia", "Ingrese una nomenclatura o JOB para buscar.")
            # self.search_field ya fue limpiado por load_csv_data(), así que no es necesario limpiarlo aquí.
            return

        # 4. (Opcional pero recomendado para UI) Restaurar el texto de búsqueda en el campo
        #    después de que load_csv_data lo haya limpiado, para que el usuario vea lo que buscó.
        self.search_field.setText(query_text)

        # 5. Continuar con la lógica de búsqueda usando query_text y el self.df recién cargado.
        # Asegúrate de que self.df no esté vacío después de load_csv_data si hubo un error de carga
        if self.df.empty:
            # Si el df está vacío (posiblemente por un error en load_csv_data que ya mostró un mensaje),
            # no tiene sentido continuar con la búsqueda.
            # Se podría mostrar otro mensaje o simplemente retornar.
            # QtWidgets.QMessageBox.information(self, "Información", "No hay datos cargados para realizar la búsqueda.")
            return

        # Buscar coincidencias en NOMENCLATURA o JOB (case-insensitive)
        # y excluir los items cuyo TIPO DE HERRAMENTAL sea "CONSUMABLE"
        # Es importante manejar el caso donde las columnas podrían no existir si el CSV está mal formado
        # aunque load_csv_data intenta crear NOMENCLATURA.
        
        # Construir condiciones de forma segura
        condition_nomenclatura = pd.Series([False] * len(self.df))
        if "NOMENCLATURA" in self.df.columns:
            condition_nomenclatura = self.df["NOMENCLATURA"].str.lower().str.contains(query_text.lower(), na=False)

        condition_job = pd.Series([False] * len(self.df))
        if "JOB" in self.df.columns:
             condition_job = self.df["JOB"].str.lower().str.contains(query_text.lower(), na=False)
        
        condition_tipo_herramental = pd.Series([True] * len(self.df)) # Por defecto, incluir todos
        if "TIPO DE HERRAMENTAL" in self.df.columns:
            condition_tipo_herramental = (self.df["TIPO DE HERRAMENTAL"].str.strip().str.upper() != "CONSUMABLE")
        else:
            # Si la columna no existe, podrías decidir si esto cuenta como no ser "CONSUMABLE"
            # o si la búsqueda debe fallar o advertir. Aquí se asume que se incluyen.
            pass


        matches = self.df[
            (condition_nomenclatura | condition_job) & condition_tipo_herramental
        ]

        if matches.empty:
            QtWidgets.QMessageBox.information(self, "No encontrado", f"No se encontró el ítem con el criterio '{query_text}'.")
            # query_text ya está en self.search_field por el setText anterior.
            return

        if len(matches) == 1:
            self.current_item_idx = matches.index[0]
            self.current_item = matches.iloc[0]
        else:
            items = []
            for idx, row in matches.iterrows():
                item_dict = row.to_dict()
                item_dict["DF_IDX"] = idx # Guardar el índice original del DataFrame
                items.append(item_dict)
            
            # Asumiendo que ItemSelectionDialog es una clase que has definido
            # dlg = ItemSelectionDialog(items, parent=self)
            # if dlg.exec_() == QtWidgets.QDialog.Accepted:
            #     sel_idx = dlg.selected_df_index # El diálogo debe devolver el DF_IDX
            #     self.current_item_idx = sel_idx
            #     self.current_item = self.df.loc[sel_idx]
            # else:
            #     # El usuario canceló la selección, query_text ya está en self.search_field.
            #     return
            # Ejemplo placeholder si ItemSelectionDialog no está disponible:
            print(f"Múltiples coincidencias ({len(matches)}), se requiere selección. Implementar ItemSelectionDialog.")
            # Por ahora, tomaremos el primer item para que el código no falle:
            self.current_item_idx = matches.index[0]
            self.current_item = matches.iloc[0]

        self.update_action_buttons()
        self.update_status_display() # Descomenta si tienes este método
        self.update_last_out_display() # Descomenta si tienes este método
        print(f"Item encontrado/seleccionado: {self.current_item}") # Para depuración

    def update_action_buttons(self):
        """
        Actualiza la visibilidad de los botones de acción según el estado del herramental.
          - Si STATUS_INOUT es "limpieza", se ocultan SURTIR e INGRESAR y se muestra LIMPIAR.
          - En otro caso, se muestran SURTIR e INGRESAR y se oculta LIMPIAR.
        """
        if self.current_item is not None and self.current_item.get("STATUS_INOUT", "").lower() == "limpieza":
            self.surtir_button.hide()
            self.ingresar_button.hide()
            self.limpiar_button.show()
        else:
            self.limpiar_button.hide()
            self.surtir_button.show()
            self.ingresar_button.show()

    def update_status_display(self):
        # Obtener y preparar el valor del estado
        status_value = str(self.current_item.get("STATUS_INOUT", "")).strip().lower()
        
        # Establecer texto y estilo según el estado
        if status_value == "in":
            text = "Dentro del Tool"
            style = ("background-color: green; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")
        elif status_value == "out":
            text = "Surtido a piso"
            style = ("background-color: orange; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")
        elif status_value == "area roja":
            text = "Bloqueado (Área Roja)"
            style = ("background-color: darkred; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")
        elif status_value == "scrap":
            text = "Bloqueado (SCRAP)"
            style = ("background-color: darkred; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")
        elif status_value == "limpieza":
            text = "Limpieza (Limpia el herramental)"
            style = ("background-color: blue; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")
        else:
            text = "No registrado"
            style = ("background-color: red; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")
        
        self.status_label.setText(text)
        self.status_label.setStyleSheet(style)

        # Mostrar "Surtido a" y "Surtido por" únicamente si el estado es "out"
        if status_value == "out":
            user_out_val = self.current_item.get("USER_OUT", "No definido")
            emp_out_val = self.current_item.get("EMPLOYEE_OUT", "No definido")
            self.user_out_label.setText(str(user_out_val))
            self.employee_out_label.setText(str(emp_out_val))
            self.user_out_label.setVisible(True)
            self.employee_out_label.setVisible(True)
        else:
            self.user_out_label.setVisible(False)
            self.employee_out_label.setVisible(False)

        # Actualizar los datos del ítem: Nomenclatura, JOB y RACK
        nomen = self.current_item.get("NOMENCLATURA", "N/A")
        job = self.current_item.get("JOB", "N/A")
        rack = self.current_item.get("RACK", "N/A")
        self.nomen_label.setText(str(nomen))
        self.job_label.setText(str(job))
        self.rack_label.setText(str(rack))

        # Mostrar "Última salida" según la información actual
        last_out = self.current_item.get("LAST_OUT", "---")
        self.last_out_label.setText(str(last_out))

        # Actualizar el indicador de stock para ítems de tipo MULTIPLE o CONSUMABLE
        tipo_inout = str(self.current_item.get("TYPE_INOUT", "")).strip().upper()
        tipo_cons = str(self.current_item.get("TYPE_CONS_INOUT", "")).strip().upper()
        if tipo_inout == "MULTIPLE" or tipo_cons == "CONSUMABLE":
            stock = self.current_item.get("MULTI_STOCK_ALL", "N/A")
            self.lblMultiStock.setText(f"Stock: {stock}")
            self.lblMultiStock.setVisible(True)
        else:
            self.lblMultiStock.setVisible(False)

    def update_last_out_display(self):
        from datetime import datetime
        try:
            # Se obtienen la NOMENCLATURA y el JOB del ítem seleccionado, en minúsculas
            nomenclatura = str(self.current_item.get("NOMENCLATURA", "")).strip().lower()
            job_val = str(self.current_item.get("JOB", "")).strip().lower()

            # Se intenta leer el historial
            df_hist = pd.read_csv(HISTORY_PATH, encoding="utf-8-sig")
        except Exception as e:
            style = ("background-color: red; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")
            self.last_out_label.setText("Última salida: Sin registro")
            self.last_out_label.setStyleSheet(style)
            return

        # Asegurarse de que la columna DATE sea tratada como texto para su correcto parseo
        df_hist["DATE"] = df_hist["DATE"].astype(str)
        
        # Normalizar las columnas relevantes para la comparación
        df_hist["NOMENCLATURA"] = df_hist["NOMENCLATURA"].astype(str).str.strip().str.lower()
        df_hist["JOB"] = df_hist["JOB"].astype(str).str.strip().str.lower()
        df_hist["MOVIMIENTO"] = df_hist["MOVIMIENTO"].astype(str).str.strip().str.lower()

        # Filtrar los registros correspondiente al ítem (buscando coincidencia en NOMENCLATURA o JOB)
        # y que tengan MOVIMIENTO EXACTO "surtir a piso"
        df_match = df_hist[
            (((df_hist["NOMENCLATURA"] == nomenclatura) | (df_hist["JOB"] == job_val)) &
            (df_hist["MOVIMIENTO"] == "surtir a piso"))
        ].copy()

        if df_match.empty:
            style = ("background-color: red; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")
            self.last_out_label.setText("Última salida: Sin registro")
            self.last_out_label.setStyleSheet(style)
            return

        # Función de parseo robusto usando varios formatos y respaldo con dateutil
        def robust_parse_date(date_str):
            from dateutil import parser
            date_str = str(date_str).strip()
            formats = [
                "%d/%m/%Y %H:%M:%S",  # Ej: 15/04/2025 11:59:30
                "%d/%m/%y %H:%M:%S",  # Ej: 15/04/25 11:59:30
                "%d/%m/%Y %H:%M",     # Ej: 15/04/2025 08:17
                "%d/%m/%y %H:%M"      # Ej: 15/04/25 08:17
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except Exception:
                    continue
            # Como respaldo, utilizar dateutil.parser
            try:
                return parser.parse(date_str, dayfirst=True)
            except Exception:
                return pd.NaT

        # Aplicar el parseo robusto a cada registro de la columna DATE
        df_match["DATE_dt"] = df_match["DATE"].apply(robust_parse_date)
        df_match = df_match[df_match["DATE_dt"].notnull()]

        if df_match.empty:
            style = ("background-color: red; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")
            self.last_out_label.setText("Última salida: Sin registro")
            self.last_out_label.setStyleSheet(style)
            return

        # Se obtiene la fecha más reciente
        last_out = df_match["DATE_dt"].max()
        if pd.isna(last_out):
            style = ("background-color: red; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")
            self.last_out_label.setText("Última salida: Sin registro")
            self.last_out_label.setStyleSheet(style)
            return

        # Cálculo de indicadores: comparación entre la fecha actual y la última salida
        now = datetime.now()
        elapsed_hours = (now - last_out).total_seconds() / 3600
        indicator_hours = self.current_item.get("LAST_OUT_INDICATOR", 0)
        current_shift = self.get_shift(now)
        last_out_shift = self.get_shift(last_out)

        if current_shift != last_out_shift:
            style = ("background-color: red; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")
        elif elapsed_hours > indicator_hours:
            style = ("background-color: orange; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")
        else:
            style = ("background-color: green; color: white; padding: 5px; "
                    "font-weight: bold; border-radius: 3px;")

        # Mostrar la fecha de la última salida formateada con año completo
        display_text = last_out.strftime("%d/%m/%Y %H:%M:%S")
        self.last_out_label.setText(f"Última salida: {display_text}")
        self.last_out_label.setStyleSheet(style)
    
    def get_shift(self, dt):
        hour = dt.hour
        if 7 <= hour < 15:
            return "T1"
        elif 15 <= hour < 23:
            return "T2"
        else:
            return "T3"

    def save_csv_data(self):
        db_lock_path = DB_PATH + ".lock"
        lock = FileLock(db_lock_path, timeout=10)
        try:
            with lock:
                # Releer el CSV actual para captar los cambios previos
                if os.path.exists(DB_PATH):
                    current_df = pd.read_csv(DB_PATH, encoding="utf-8-sig")
                else:
                    current_df = pd.DataFrame()
                
                # Si el CSV no está vacío y tiene la columna clave "NOMENCLATURA", haremos la fusión.
                if not current_df.empty and "NOMENCLATURA" in current_df.columns:
                    # ELIMINAR duplicados para evitar errores al establecer el índice.
                    current_df = current_df.drop_duplicates(subset=["NOMENCLATURA"])
                    new_df = self.df.drop_duplicates(subset=["NOMENCLATURA"])
                    
                    # Establecer "NOMENCLATURA" como índice en ambos DataFrames
                    current_df.set_index("NOMENCLATURA", inplace=True)
                    new_df.set_index("NOMENCLATURA", inplace=True)
                    
                    # Para las columnas comunes, si el CSV tiene dtype numérico y new_df es object, forzamos la conversión.
                    common_cols = current_df.columns.intersection(new_df.columns)
                    for col in common_cols:
                        if pd.api.types.is_numeric_dtype(current_df[col]) and new_df[col].dtype == object:
                            new_df[col] = pd.to_numeric(new_df[col], errors="coerce")
                    
                    # Actualizar las filas existentes en current_df con las de new_df.
                    current_df.update(new_df)
                    # Agregar las nuevas filas que estén en new_df pero no en current_df.
                    new_rows = new_df.loc[~new_df.index.isin(current_df.index)]
                    merged_df = pd.concat([current_df, new_rows])
                    merged_df.reset_index(inplace=True)
                    updated_df = merged_df.copy()
                else:
                    updated_df = self.df.copy()
                
                # Escribir el DataFrame actualizado en el CSV
                updated_df.to_csv(DB_PATH, index=False, encoding="utf-8-sig")
                
                # Opcional: actualizar internamente self.df con el contenido final
                self.df = updated_df.copy()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo guardar el archivo CSV:\n{e}")

    # -----------------------------------------------------------------------------
    # Función para el movimiento de surtido ("Surtir a piso")
    # -----------------------------------------------------------------------------
    def surtir_action(self):
        print("[DEBUG] Iniciando surtir_action()")
        # Verificar que exista un ítem seleccionado
        if self.current_item is None:
            print("[DEBUG] No hay ítem seleccionado.")
            QtWidgets.QMessageBox.warning(self, "Advertencia", "Primero busque un ítem.")
            return

        # Validar que el archivo de la base de datos exista
        if not os.path.isfile(DB_PATH):
            print("[DEBUG] DB_PATH no existe:", DB_PATH)
            QtWidgets.QMessageBox.critical(self, "Error", f"El archivo de base de datos no se encontró:\n{DB_PATH}")
            return

        # Antes de proceder, validar que el mantenimiento esté vigente.
        try:
            df_db = pd.read_csv(DB_PATH, encoding="utf-8-sig")
            print("[DEBUG] DB leído para validar mantenimiento. Shape:", df_db.shape)
            # Buscar el registro con la nomenclatura seleccionada
            registro = df_db[df_db["NOMENCLATURA"].astype(str).str.strip() == str(self.current_item.get("NOMENCLATURA","")).strip()]
            if not registro.empty:
                # Convertir la fecha en "ULTIMO_MANTENIMIENTO" a datetime.
                # Asumimos el formato "%d/%m/%Y"; ajústalo si es necesario.
                prox_mant = pd.to_datetime(registro.iloc[0]["PROXIMO_MANTENIMIENTO"], format="%d/%m/%Y", errors="coerce")
                now_actual = pd.to_datetime(datetime.now().strftime("%d/%m/%Y"), format="%d/%m/%Y")
                print("[DEBUG] Último mantenimiento:", prox_mant, "Fecha actual:", now_actual)
                if pd.isna(prox_mant) or prox_mant < now_actual:
                    QtWidgets.QMessageBox.warning(
                        self,
                        "Mantenimiento vencido",
                        "No se puede surtir este herramental debido a que el mantenimiento está vencido o no hay registro, favor de realizar mantenimiento."
                    )
                    print("[DEBUG] Mantenimiento vencido. Proceso cancelado.")
                    return
            else:
                print("[DEBUG] No se encontró registro para la nomenclatura:", self.current_item.get("NOMENCLATURA",""))
        except Exception as e:
            print("[DEBUG] Error al validar mantenimiento:", e)
            QtWidgets.QMessageBox.warning(self, "Error", f"Error al validar mantenimiento:\n{e}")
            return

        # Obtener el estado actual del ítem, la nomenclatura y el JOB
        current_status = str(self.current_item.get("STATUS_INOUT", "")).strip().lower()
        nomenclatura = str(self.current_item.get("NOMENCLATURA", "")).strip()
        job = str(self.current_item.get("JOB", "")).strip()
        print(f"[DEBUG] current_status: {current_status}, nomenclatura: {nomenclatura}, job: {job}")

        # Bloquear si el estado es "area roja" o "scrap"
        if current_status in ["area roja", "scrap", "limpieza"]:
            print("[DEBUG] Estado prohibido para surtir:", current_status)
            QtWidgets.QMessageBox.warning(
                self,
                "Advertencia",
                f"El ítem no puede ser surtido porque su estado es '{current_status.upper()}'."
            )
            return

        # Función interna para determinar el estado efectivo basada en el historial
        def get_effective_status(nomenclatura, job, current_status):
            print("[DEBUG] get_effective_status() llamado con nomenclatura:", nomenclatura, "job:", job, "current_status:", current_status)
            try:
                df_hist = pd.read_csv(HISTORY_PATH, encoding="utf-8-sig")
                print("[DEBUG] Historial leído. Shape:", df_hist.shape)
            except Exception as e:
                print("[DEBUG] Error al leer historial:", e)
                return current_status

            if df_hist.empty:
                print("[DEBUG] Historial está vacío.")
                return current_status

            # Normalizar columnas
            df_hist["NOMENCLATURA"] = df_hist["NOMENCLATURA"].astype(str).str.lower().str.strip()
            df_hist["JOB"] = df_hist["JOB"].astype(str).str.lower().str.strip()
            df_hist["MOVIMIENTO"] = df_hist["MOVIMIENTO"].astype(str).str.strip()

            # Filtrar por NOMENCLATURA (o JOB si no se encuentra)
            df_nomen = df_hist[df_hist["NOMENCLATURA"] == nomenclatura.lower()].copy()
            if df_nomen.empty and job:
                print("[DEBUG] No se encontró por NOMENCLATURA; intentando por JOB")
                df_nomen = df_hist[df_hist["JOB"] == job.lower()].copy()
            if df_nomen.empty:
                print("[DEBUG] No se encontraron registros relevantes en el historial.")
                return current_status

            try:
                df_nomen["DATE_dt"] = pd.to_datetime(df_nomen["DATE"], format="%d/%m/%y %H:%M:%S", errors="coerce")
                df_nomen = df_nomen[df_nomen["DATE_dt"].notnull()]
                print("[DEBUG] Conversión de fechas completa. Shape:", df_nomen.shape)
            except Exception as e:
                print("[DEBUG] Error al convertir fecha:", e)
                return current_status

            if df_nomen.empty:
                return current_status

            ultimo_reg = df_nomen.sort_values("DATE_dt").iloc[-1]
            ultimo_mov = ""
            if pd.notna(ultimo_reg["MOVIMIENTO"]):
                ultimo_mov = ultimo_reg["MOVIMIENTO"].strip()
            print("[DEBUG] Último movimiento en historial:", ultimo_mov)

            # Doble validación:
            if current_status == "out" and ultimo_mov == "Ingresar a Tool":
                print("[DEBUG] Se devuelve 'in' por doble validación: current_status=out y último movimiento 'Ingresar a Tool'")
                return "in"
            elif current_status == "in" and ultimo_mov == "Surtir a piso":
                print("[DEBUG] Se devuelve 'out' por doble validación: current_status=in y último movimiento 'Surtir a piso'")
                return "out"
            print("[DEBUG] Se devuelve current_status sin cambios:", current_status)
            return current_status

        effective_status = get_effective_status(nomenclatura, job, current_status)
        print("[DEBUG] effective_status obtenido:", effective_status)
        prefill_comment = ""

        # Procesar la acción según el estado efectivo, forzando la actualización a 'out'
        now = datetime.now().strftime("%d/%m/%y %H:%M")
        if effective_status == "in":
            print("[DEBUG] Caso 'in': se actualiza STATUS_INOUT a 'out' y asigna LAST_OUT:", now)
            self.df.loc[self.current_item_idx, "STATUS_INOUT"] = "out"
            self.df.loc[self.current_item_idx, "LAST_OUT"] = now
        else:
            print("[DEBUG] Caso 'out': el historial sugiere que el herramental ya fue ingresado, se forzará la actualización a 'out'.")
            self.df.loc[self.current_item_idx, "STATUS_INOUT"] = "out"
            self.df.loc[self.current_item_idx, "LAST_OUT"] = now

        print("[DEBUG] Después de la actualización, STATUS_INOUT en self.df (fila {}): {}".format(
            self.current_item_idx, self.df.loc[self.current_item_idx, "STATUS_INOUT"]))

        # Obtener JOB desde DB_PATH filtrando por NOMENCLATURA
        job_value = ""
        try:
            df_db = pd.read_csv(DB_PATH, encoding="utf-8-sig")
            print("[DEBUG] DB leído para obtener JOB. Shape:", df_db.shape)
            registro = df_db[df_db["NOMENCLATURA"].astype(str).str.strip() == nomenclatura]
            if not registro.empty:
                job_value = str(registro.iloc[0]["JOB"])
            print("[DEBUG] job_value obtenido:", job_value)
        except Exception as e:
            print("[DEBUG] Error al obtener JOB desde DB:", e)

        # Actualizar LAST_OUT nuevamente para mayor consistencia.
        now = datetime.now().strftime("%d/%m/%y %H:%M")
        self.df.loc[self.current_item_idx, "LAST_OUT"] = now
        print("[DEBUG] LAST_OUT actualizado a:", now)

        # Solicitar Número de Nómina y LINEA
        emp_num, ok = QtWidgets.QInputDialog.getText(self, "Número de Nómina", "Ingrese el número de nómina del empleado:")
        if not ok or emp_num.strip() == "":
            print("[DEBUG] Cancelación al ingresar número de nómina.")
            return
        user_mfg = emp_num.strip()
        print("[DEBUG] Número de Nómina recibido:", user_mfg)

        linea, ok_linea = QtWidgets.QInputDialog.getText(self, "LINEA", "Ingrese la LINEA:")
        if not ok_linea:
            print("[DEBUG] Cancelación al ingresar LINEA.")
            return

        # Solicitar Estado y Comentario mediante diálogo personalizado
        dlg = EstadoComentarioDialog(self)
        if prefill_comment:
            dlg.comentario_edit.setText(prefill_comment)
        if dlg.exec_() == QtWidgets.QDialog.Accepted:
            estado, comentario = dlg.get_data()
            print(f"[DEBUG] Diálogo finalizado. Estado: '{estado}', Comentario: '{comentario}'")
        else:
            print("[DEBUG] Diálogo de estado/comentario cancelado.")
            estado, comentario = "", ""

        # Registrar la acción en el historial (write_history maneja su propio FileLock)
        print("[DEBUG] Registrando la acción en el historial con write_history()")
        write_history(
            self.user_alias,
            nomenclatura,
            job_value,
            linea,
            user_mfg,
            estado,
            "Surtir a piso",
            comentario,
            ""
        )

        # Guardar los cambios en el archivo de base de datos realizando relectura y fusión (merge)
        print("[DEBUG] Llamando a save_csv_data() para guardar los cambios.")
        self.save_csv_data()

        # Actualizar la interfaz de usuario
        print("[DEBUG] Actualizando la interfaz (load_csv_data() y search_item()).")
        self.search_item()
        print("[DEBUG] surtir_action() finalizado.")

    # -----------------------------------------------------------------------------
    # Función para el movimiento de ingreso ("Ingresar a Tool")
    # -----------------------------------------------------------------------------
    def ingresar_action(self):
        if self.current_item is None:
            QtWidgets.QMessageBox.warning(self, "Advertencia", "Primero busque un ítem.")
            return

        if not os.path.isfile(DB_PATH):
            QtWidgets.QMessageBox.critical(self, "Error", f"El archivo de base de datos no se encontró:\n{DB_PATH}")
            return

        # Obtener el estado actual, nomenclatura y JOB del ítem seleccionado
        current_status = str(self.current_item.get("STATUS_INOUT", "")).strip().lower()
        nomenclatura = str(self.current_item.get("NOMENCLATURA", "")).strip()
        job = str(self.current_item.get("JOB", "")).strip()

        # Bloquear si el ítem se encuentra en "AREA ROJA" o "SCRAP"
        if current_status in ["area roja", "scrap"]:
            QtWidgets.QMessageBox.warning(
                self,
                "Advertencia",
                f"El ítem no puede ser ingresado porque está bloqueado ({current_status.upper()})."
            )
            return

        # Función interna para obtener el estado efectivo de ingreso basado en el historial (HISTORY_PATH)
        def get_effective_status(nomenclatura, job, current_status):
            try:
                df_hist = pd.read_csv(HISTORY_PATH, encoding="utf-8-sig")
            except Exception as e:
                print("Error al leer historial:", e)
                return current_status

            if df_hist.empty:
                return current_status

            df_hist["NOMENCLATURA"] = df_hist["NOMENCLATURA"].astype(str).str.lower().str.strip()
            df_hist["JOB"] = df_hist["JOB"].astype(str).str.lower().str.strip()
            df_hist["MOVIMIENTO"] = df_hist["MOVIMIENTO"].astype(str).str.strip()

            df_nomen = df_hist[df_hist["NOMENCLATURA"] == nomenclatura.lower()].copy()
            if df_nomen.empty and job:
                df_nomen = df_hist[df_hist["JOB"] == job.lower()].copy()

            if df_nomen.empty:
                return current_status

            try:
                df_nomen["DATE_dt"] = pd.to_datetime(df_nomen["DATE"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
                df_nomen = df_nomen[df_nomen["DATE_dt"].notnull()]
            except Exception as e:
                print("Error al convertir fecha:", e)
                return current_status

            if df_nomen.empty:
                return current_status

            ultimo_reg = df_nomen.sort_values("DATE_dt").iloc[-1]
            ultimo_mov = ""
            if pd.notna(ultimo_reg["MOVIMIENTO"]):
                ultimo_mov = ultimo_reg["MOVIMIENTO"].strip()

            # Doble validación para ingreso basada en historial:
            if current_status == "out" and ultimo_mov == "Surtir a piso":
                return "out"
            elif current_status == "in" and ultimo_mov == "Ingresar a Tool":
                return "in"
            return current_status

        effective_status = get_effective_status(nomenclatura, job, current_status)
        prefill_comment = ""

        # Procesar la acción de ingreso:
        # En el nuevo flujo, al ingresar el herramental se asigna el estado "limpieza"
        if effective_status == "out":
            now = datetime.now().strftime("%d/%m/%y %H:%M")
            self.df.at[self.current_item_idx, "STATUS_INOUT"] = "limpieza"
            self.df.at[self.current_item_idx, "LAST_OUT"] = now
        else:
            QtWidgets.QMessageBox.information(self, "Aviso", "Herramental sin registro de surtido, comentalo")
            prefill_comment = "Herramental sin registro de surtido"
            now = datetime.now().strftime("%d/%m/%y %H:%M")
            self.df.at[self.current_item_idx, "LAST_OUT"] = now
            self.df.at[self.current_item_idx, "STATUS_INOUT"] = "limpieza"

        # Reiniciar el contenido de USER_OUT en la base de datos
        self.df["USER_OUT"] = self.df["USER_OUT"].fillna("").astype(str)
        self.df.at[self.current_item_idx, "USER_OUT"] = ""

        # Exclusivo para herramental: Solicitar Número de Nómina y LINEA
        emp_num, ok = QtWidgets.QInputDialog.getText(
            self, "Número de Nómina", "Ingrese el número de nómina del empleado:"
        )
        if not ok or emp_num.strip() == "":
            return
        user_mfg = emp_num.strip()
        linea, ok_linea = QtWidgets.QInputDialog.getText(self, "LINEA", "Ingrese la LINEA:")
        if not ok_linea:
            return

        # Obtener JOB desde DB_PATH filtrando por Nomenclatura
        job_value = ""
        try:
            df_db = pd.read_csv(DB_PATH, encoding="utf-8-sig")
            registro = df_db[df_db["NOMENCLATURA"].astype(str).str.strip() == nomenclatura]
            if not registro.empty:
                job_value = str(registro.iloc[0]["JOB"])
        except Exception as e:
            print("Error al obtener JOB desde DB:", e)

        # Solicitar Estado y Comentario mediante diálogo personalizado
        dlg = EstadoComentarioDialog(self)
        if prefill_comment:
            dlg.comentario_edit.setText(prefill_comment)
        if dlg.exec_() == QtWidgets.QDialog.Accepted:
            estado, comentario = dlg.get_data()
        else:
            estado, comentario = "", ""

        # Registrar en el historial
        write_history(
            self.user_alias,
            nomenclatura,
            job_value,
            linea,
            user_mfg,
            estado,
            "Ingresar a Tool",
            comentario,
            ""
        )

        self.save_status_change()
        self.update_action_buttons()
        self.search_item()


    def limpiar_action(self):
        """
        Al presionar "LIMPIAR", se actualiza el estado del herramental a "in",
        indicando que el herramental está limpio y vuelve a estar disponible;
        además, se registra el movimiento de limpieza en el historial.
        """
        if self.current_item is not None and self.current_item.get("STATUS_INOUT", "").lower() == "limpieza":
            # Actualizar el estado a "in" en el DataFrame
            self.df.at[self.current_item_idx, "STATUS_INOUT"] = "in"
            self.status_label.setText("Dentro del Tool")
            self.status_label.setStyleSheet(
                "background-color: green; color: white; padding: 5px; font-weight: bold; border-radius: 3px;"
            )
            
            # Obtener datos del ítem para el historial
            nomenclatura = str(self.current_item.get("NOMENCLATURA", ""))
            job_value = str(self.current_item.get("JOB", ""))
            linea = str(self.current_item.get("LINEA", ""))
            user_mfg = str(self.current_item.get("USER MFG", ""))
            
            # Comentario y estado a registrar en el historial.
            comentario = "Herramental limpiado y colocado en su localidad."
            estado = "in"  # Estado final registrado
            
            # Registrar en el historial del movimiento de limpieza.
            write_history(
                self.user_alias,
                nomenclatura,
                job_value,
                linea,
                user_mfg,
                estado,
                "Limpiar Herramental",
                comentario,
                ""
            )
        self.update_action_buttons()
            # Guardar el cambio en el archivo CSV (save_status_change ya utiliza FileLock)
        self.save_status_change()
        self.search_item()


    def modificar_action(self):
        # Verificar que exista un ítem seleccionado
        if self.current_item is None:
            QtWidgets.QMessageBox.warning(self, "Advertencia", "Primero busque un ítem.")
            return

        try:
            # Validar que el archivo de base de datos exista (DB_PATH es global)
            if not os.path.isfile(DB_PATH):
                QtWidgets.QMessageBox.critical(self, "Error", f"El archivo de base de datos no se encontró:\n{DB_PATH}")
                return

            # Leer la versión más reciente del archivo para evitar sobreescritura.
            self.df = pd.read_csv(DB_PATH, encoding="utf-8-sig")

            # Verificar privilegios del usuario usando USERS_DB_PATH (global)
            users_df = pd.read_csv(USERS_DB_PATH, encoding="utf-8-sig")
            match = users_df[users_df["ALIAS"].str.lower() == self.user_alias.lower()].copy()
            if match.empty:
                QtWidgets.QMessageBox.critical(self, "Error", "Usuario no encontrado en la base de datos.")
                return
            update_permission = str(match.iloc[0].get("UPDATE_OBJECT", "no")).strip().lower()
            if update_permission != "yes":
                QtWidgets.QMessageBox.critical(self, "Error", "Privilegios insuficientes para modificar el estado.")
                return
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Error al verificar privilegios:\n{e}")
            return

        # Selección del tipo de modificación
        mod_options = ["Datos del item", "Estado de surtido"]
        mod_choice, ok = QtWidgets.QInputDialog.getItem(
            self, "Modificar", "Seleccione el tipo de modificación:", mod_options, 0, False)
        if not ok:
            return

        # Valores base a usar para el historial. Se extraen del ítem actual.
        nomenclatura = str(self.current_item.get("NOMENCLATURA", ""))
        job_value = str(self.current_item.get("JOB", ""))
        # Si el ítem tiene información de línea y usuario de manufactura, se extraen;
        # de lo contrario se envían como cadena vacía.
        linea = str(self.current_item.get("LINEA", ""))
        user_mfg = str(self.current_item.get("USER MFG", ""))
        comentario = ""

        if mod_choice == "Estado de surtido":
            status_options = [
                "in (DENTRO DEL TOOL)",
                "out (SURTIDO A PISO)",
                "AREA ROJA",
                "SCRAP",
                "limpieza"
            ]
            new_status, ok_status = QtWidgets.QInputDialog.getItem(
                self, "Modificar Estado", "Seleccione el nuevo estado:", status_options, 0, False)
            if not ok_status:
                return

            # Actualizar la columna STATUS_INOUT en el DataFrame según la opción elegida.
            if new_status.lower() == "in (dentro del tool)":
                self.df.at[self.current_item_idx, "STATUS_INOUT"] = "in"
            elif new_status.lower() == "out (surtido a piso)":
                self.df.at[self.current_item_idx, "STATUS_INOUT"] = "out"
            elif new_status.upper() == "AREA ROJA":
                self.df.at[self.current_item_idx, "STATUS_INOUT"] = "area roja"
            elif new_status.upper() == "SCRAP":
                self.df.at[self.current_item_idx, "STATUS_INOUT"] = "scrap"
            elif new_status.lower() == "limpieza":
                self.df.at[self.current_item_idx, "STATUS_INOUT"] = "limpieza"
            else:
                QtWidgets.QMessageBox.warning(self, "Error", "Estado no válido seleccionado.")
                return

            QtWidgets.QMessageBox.information(self, "Éxito", "Estado modificado con éxito.")
            # Solicitar un comentario opcional para el historial.
            comentario, ok_coment = QtWidgets.QInputDialog.getText(
                self, "Comentario", "Ingrese comentario (opcional):")
            if not ok_coment:
                comentario = ""

            # Registrar el historial de la modificación con la función robusta.
            write_history(
                self.user_alias,
                nomenclatura,
                job_value,
                linea,
                user_mfg,
                new_status,              # Estado
                "Modificar estado de surtido",  # Movimiento
                comentario,
                ""
            )
        elif mod_choice == "Datos del item":
            # Mostrar un diálogo único para editar todos los campos del ítem.
            dialog = EditItemDialog(self.current_item, self)
            if dialog.exec_() == QtWidgets.QDialog.Accepted:
                new_data = dialog.getData()
                # Actualizar cada campo en la fila correspondiente del DataFrame.
                for field, value in new_data.items():
                    self.df.at[self.current_item_idx, field] = value
                QtWidgets.QMessageBox.information(self, "Éxito", "Datos del item modificados con éxito.")
                # Actualizar la nomenclatura en caso de que haya cambiado.
                nomenclatura = str(self.df.at[self.current_item_idx, "NOMENCLATURA"])
                comentario, ok_coment = QtWidgets.QInputDialog.getText(
                    self, "Comentario", "Ingrese comentario (opcional):")
                if not ok_coment:
                    comentario = ""
                write_history(
                    self.user_alias,
                    nomenclatura,
                    job_value,
                    linea,
                    user_mfg,
                    "",  # sin estado específico para datos
                    "Modificar datos del item",
                    comentario,
                    ""
                )
            else:
                QtWidgets.QMessageBox.information(self, "Cancelado", "Modificación cancelada.")
                return

        # Guardar los cambios en el archivo de base de datos (DB_PATH es global)
        # Se utiliza FileLock para evitar conflictos de escritura simultánea
        # y se realiza una relectura y fusión (merge) para incorporar cambios concurrentes.
        db_lock_path = DB_PATH + ".lock"
        lock = FileLock(db_lock_path, timeout=10)
        try:
            with lock:
                # Releer el CSV actual para captar los cambios de otros usuarios
                if os.path.exists(DB_PATH):
                    current_df = pd.read_csv(DB_PATH, encoding="utf-8-sig")
                else:
                    current_df = pd.DataFrame()

                # Si el CSV no está vacío y tiene la columna clave "NOMENCLATURA", se fusiona
                if not current_df.empty and "NOMENCLATURA" in current_df.columns:
                    # Establecer "NOMENCLATURA" como índice en ambos DataFrames para facilitar la comparación
                    current_df.set_index("NOMENCLATURA", inplace=True)
                    new_df = self.df.copy()
                    new_df.set_index("NOMENCLATURA", inplace=True)

                    # Actualizar las filas existentes con las modificaciones de new_df
                    current_df.update(new_df)
                    # Agregar las filas nuevas que están en new_df pero no en current_df
                    new_rows = new_df.loc[~new_df.index.isin(current_df.index)]
                    merged_df = pd.concat([current_df, new_rows])
                    merged_df.reset_index(inplace=True)
                    updated_df = merged_df.copy()
                else:
                    updated_df = self.df.copy()

                # Escribir el DataFrame fusionado en el CSV
                updated_df.to_csv(DB_PATH, index=False, encoding="utf-8-sig")
                # Opcional: Actualizar self.df con el contenido final
                self.df = updated_df.copy()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo guardar el archivo CSV:\n{e}")
            return

        # Refrescar la vista: recargar datos y actualizar detalles.
        self.load_csv_data()
        self.current_item = self.df.loc[self.current_item_idx].to_dict()
        self.update_status_display()
        self.update_last_out_display()
        self.update_action_buttons()
        
    def save_status_change(self):
        # Definimos la ruta del lock a partir de DB_PATH
        lock_path = DB_PATH + ".lock"
        lock = FileLock(lock_path, timeout=10)  # Timeout de 10 segundos para adquirir el lock
        try:
            with lock:
                self.df.to_csv(DB_PATH, index=False, encoding="utf-8-sig")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Error al guardar el cambio de estado:\n{e}")

# ================================================================
# Window6Page, Window7Page, Window8Page y Window9Page
# ================================================================

class ExpirationAlertDialog(QtWidgets.QDialog):
    """
    Diálogo para mostrar los SKIDs que están próximos a expirar o ya han expirado,
    y que aún se encuentran en stock. Emite una señal al hacer doble clic en una fila.
    """
    searchRequested = QtCore.pyqtSignal(str, str)

    def __init__(self, df_expiring_items, parent=None):
        super().__init__(parent)
        # Realizamos una copia para no modificar el DataFrame original.
        self.df = df_expiring_items.copy()
        self.setWindowTitle(f"Alerta de Expiración ({len(self.df)} ítems)")
        self.resize(900, 600)
        
        layout = QtWidgets.QVBoxLayout(self)
        
        info_label = QtWidgets.QLabel(
            f"Doble clic en una fila para buscar el Item.\n"
            f"Ítems próximos a vencer (<= {EXPIRATION_ALERT_DAYS} días) o caducados en stock:"
        )
        info_label.setWordWrap(True)
        layout.addWidget(info_label)
        
        # Configuración de la tabla
        self.table = QtWidgets.QTableWidget(self)
        headers = ["Skid", "Item", "Descripción", "Fecha Expiración", "Días Restantes", "Status Surtido"]
        self.table.setColumnCount(len(headers))
        self.table.setHorizontalHeaderLabels(headers)
        self.table.setRowCount(len(self.df))
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.table.setSortingEnabled(True)
        self.table.itemDoubleClicked.connect(self.handle_double_click)
        
        current_date = datetime.now()

        # Población de la tabla usando la copia del DataFrame
        for table_row_index, df_index in enumerate(list(self.df.index)):
            row = self.df.loc[df_index]

            skid_val = str(row.get("Skid", "N/A"))
            item_val = str(row.get("Item", "N/A"))
            desc_val = str(row.get("Description", "N/A"))
            fecha_val = row.get("Ifecha", pd.NaT)  # Se asume que ya es datetime
            days_remaining = row.get("DaysRemaining", None)
            status_surtido = str(row.get("Status_de_surtido", "En stock"))
            
            fecha_str = fecha_val.strftime('%Y-%m-%d') if pd.notnull(fecha_val) else "Error Fecha"
            days_str = str(int(days_remaining)) if days_remaining is not None else "N/A"
            
            # Celda para días restantes
            days_item = QtWidgets.QTableWidgetItem()
            if days_remaining is not None:
                days_item.setData(QtCore.Qt.DisplayRole, int(days_remaining))
            else:
                days_item.setData(QtCore.Qt.DisplayRole, days_str)
            
            # Definir color de fondo según la condición
            bg_color = QtGui.QColor("white")
            if days_remaining is not None:
                if days_remaining < 0:
                    bg_color = QtGui.QColor("#f8d7da")
                elif days_remaining <= EXPIRATION_ALERT_DAYS:
                    bg_color = QtGui.QColor("#fff3cd")
                else:
                    bg_color = QtGui.QColor("#d4edda")
            
            col_data_items = [
                QtWidgets.QTableWidgetItem(skid_val),
                QtWidgets.QTableWidgetItem(item_val),
                QtWidgets.QTableWidgetItem(desc_val),
                QtWidgets.QTableWidgetItem(fecha_str),
                days_item,
                QtWidgets.QTableWidgetItem(status_surtido)
            ]
            
            # Asignar cada celda y aplicar el fondo
            for j, cell_item in enumerate(col_data_items):
                cell_item.setBackground(bg_color)
                if j == 4:  # Columna de días, alinear a la derecha
                    cell_item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                self.table.setItem(table_row_index, j, cell_item)
        
        self.table.resizeColumnsToContents()
        header_view = self.table.horizontalHeader()
        header_view.setSectionResizeMode(2, QtWidgets.QHeaderView.Stretch)
        for col in [0, 1, 3, 4, 5]:
            header_view.setSectionResizeMode(col, QtWidgets.QHeaderView.ResizeToContents)
        
        layout.addWidget(self.table)
        
        # Botón cerrar
        btn_layout = QtWidgets.QHBoxLayout()
        btn_layout.addStretch()
        self.btn_close = QtWidgets.QPushButton("Cerrar")
        self.btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_close)
        layout.addLayout(btn_layout)

    def handle_double_click(self, item):
        """Al hacer doble clic, se emiten los datos de búsqueda."""
        if item is None:
            return
        row_index = item.row()
        try:
            skid_item = self.table.item(row_index, 0)
            item_item = self.table.item(row_index, 1)
            if skid_item and item_item:
                skid_val = skid_item.text()
                item_val = item_item.text()
                print(f"Doble clic: Item={item_val}, Skid={skid_val}")
                self.searchRequested.emit(item_val, skid_val)
                self.accept()
            else:
                print("Error: No se encontraron datos de Skid/Item en la fila.")
        except Exception as e:
            print(f"Error en handle_double_click: {e}")

class ValidationDialog(QtWidgets.QDialog):
    def __init__(self, df_changes, parent=None):
        super().__init__(parent)
        self.df_changes = df_changes.copy()
        # Preparar las columnas de fecha
        if 'Ifecha' in self.df_changes.columns:
            self.df_changes['Ifecha_dt'] = pd.to_datetime(self.df_changes['Ifecha'], errors='coerce')
            self.df_changes['Ifecha_str'] = self.df_changes['Ifecha_dt'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna("No definida")
        else:
            self.df_changes['Ifecha_dt'] = pd.NaT
            self.df_changes['Ifecha_str'] = "No definida"

        self.setWindowTitle("Validación de Actualización de SKIDs")
        self.resize(1150, 600)
        layout = QtWidgets.QVBoxLayout(self)

        # Definir las columnas que se mostrarán
        display_columns = [
            "Operation", "Skid", "Item", "Description", "Warehouse",
            "Location", "Ifecha_str", "On Hand", "OnHandDiff", "Std",
            "Status_de_surtido", "ChangedFields", "Company"
        ]
        # Solo se usan las columnas presentes en el DataFrame
        actual_display_columns = [col for col in display_columns if col in self.df_changes.columns]
        headers = ["Seleccionar"] + [col.replace('_str', '') for col in actual_display_columns]

        self.table = QtWidgets.QTableWidget(self)
        self.table.setColumnCount(len(headers))
        self.table.setHorizontalHeaderLabels(headers)
        self.table.setRowCount(len(self.df_changes))
        self.table.setSortingEnabled(True)

        # Poblamos la tabla
        for i, row in self.df_changes.iterrows():
            # Columna de selección: checkbox
            checkbox_item = QtWidgets.QTableWidgetItem()
            if row.get("Operation") in ["Actualizar", "Agregar"]:
                checkbox_item.setCheckState(QtCore.Qt.Checked)
            else:
                checkbox_item.setCheckState(QtCore.Qt.Unchecked)
            self.table.setItem(i, 0, checkbox_item)

            changed_fields_str = str(row.get("ChangedFields", ""))
            changed_fields = [f.strip() for f in changed_fields_str.split(',') if f.strip()]

            # Rellenar las columnas restantes
            for j, col_name in enumerate(actual_display_columns):
                cell_value = row.get(col_name, "")
                if col_name == 'Std' and isinstance(cell_value, (int, float)):
                    cell_str = f"${cell_value:,.2f}"
                elif col_name == 'OnHandDiff' and pd.notna(cell_value):
                    try:
                        diff_val = float(cell_value)
                        cell_str = f"{diff_val:+.0f}" if diff_val != 0 else "0"
                    except (ValueError, TypeError):
                        cell_str = str(cell_value)
                else:
                    cell_str = str(cell_value)
                cell_item = QtWidgets.QTableWidgetItem(cell_str)
                
                # Determinar el color de fondo según la operación y los campos modificados
                bg_color = None
                operation = row.get("Operation", "")
                col_name_for_check = col_name.replace('_str', '')
                if operation == "Actualizar":
                    if col_name_for_check in changed_fields:
                        bg_color = QtGui.QColor("yellow")
                    if col_name_for_check == "On Hand" and "On Hand" in changed_fields:
                        on_hand_diff = row.get("OnHandDiff")
                        if pd.notna(on_hand_diff):
                            try:
                                diff = float(on_hand_diff)
                                bg_color = QtGui.QColor("lightgreen") if diff > 0 else QtGui.QColor("lightcoral")
                            except (ValueError, TypeError):
                                pass
                elif operation == "Agregar":
                    bg_color = QtGui.QColor("#e0ffe0")
                elif operation == "Marcar como Surtido":
                    bg_color = QtGui.QColor("#f5f5f5")
                
                if bg_color:
                    cell_item.setBackground(bg_color)
                if col_name_for_check in ["On Hand", "OnHandDiff", "Std"]:
                    cell_item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                self.table.setItem(i, j + 1, cell_item)

        self.table.resizeColumnsToContents()
        header = self.table.horizontalHeader()
        try:
            # Hacer que la descripción se expanda
            header.setSectionResizeMode(actual_display_columns.index("Description") + 1, QtWidgets.QHeaderView.Stretch)
        except ValueError:
            pass
        try:
            header.setSectionResizeMode(actual_display_columns.index("ChangedFields") + 1, QtWidgets.QHeaderView.Stretch)
        except ValueError:
            pass
        header.setSectionResizeMode(0, QtWidgets.QHeaderView.ResizeToContents)

        layout.addWidget(self.table)

        # Botones inferiores
        btn_layout = QtWidgets.QHBoxLayout()
        self.select_all_checkbox = QtWidgets.QCheckBox("Seleccionar Todos / Ninguno")
        self.select_all_checkbox.stateChanged.connect(self.toggle_select_all)
        btn_layout.addWidget(self.select_all_checkbox)
        btn_layout.addStretch()
        self.btn_confirmar = QtWidgets.QPushButton("Confirmar Cambios Seleccionados")
        self.btn_confirmar.clicked.connect(self.confirmar)
        btn_layout.addWidget(self.btn_confirmar)
        self.btn_cancelar = QtWidgets.QPushButton("Cancelar")
        self.btn_cancelar.clicked.connect(self.reject)
        btn_layout.addWidget(self.btn_cancelar)
        layout.addLayout(btn_layout)

        self.selected_indices = []

    def toggle_select_all(self, state):
        """Marca o desmarca todos los checkbox según el estado del checkbox global."""
        check_state = QtCore.Qt.Checked if state == QtCore.Qt.Checked else QtCore.Qt.Unchecked
        for i in range(self.table.rowCount()):
            item = self.table.item(i, 0)
            if item:
                item.setCheckState(check_state)

    def confirmar(self):
        """Recorre la tabla para extraer las filas seleccionadas y devuelve los índices originales."""
        self.selected_indices = []
        for i in range(self.table.rowCount()):
            cell = self.table.item(i, 0)
            if cell is not None and cell.checkState() == QtCore.Qt.Checked:
                # Usamos el índice original del DataFrame
                original_index = self.df_changes.index[i]
                self.selected_indices.append(original_index)
        if not self.selected_indices:
            QtWidgets.QMessageBox.warning(self, "Sin Selección", "No has seleccionado cambios.")
            return
        print("Índices originales seleccionados:", self.selected_indices)
        self.accept()
class EditSkidDialog(QtWidgets.QDialog):
    """
    Diálogo para editar manualmente los datos de un registro de SKID.
    Se pre-cargan los valores actuales y, al aceptar, se devuelve un
    diccionario con los datos actualizados.
    """
    def __init__(self, record, parent=None):
        """
        record: dict con la información actual del registro.
        parent: Widget padre.
        """
        super().__init__(parent)
        self.setWindowTitle(f"Editar SKID: {record.get('Skid', 'N/A')}")
        self.resize(450, 400)
        self.record = record.copy()  # Guardamos una copia del registro original

        # Layout principal
        layout = QtWidgets.QVBoxLayout(self)

        # Formulario para edición
        form_layout = QtWidgets.QFormLayout()
        form_layout.setLabelAlignment(QtCore.Qt.AlignRight)

        # Campo SKID (solo lectura)
        self.edit_skid = QtWidgets.QLineEdit(record.get("Skid", ""))
        self.edit_skid.setReadOnly(True)
        form_layout.addRow("Skid:", self.edit_skid)

        # Campo Item
        self.edit_item = QtWidgets.QLineEdit(record.get("Item", ""))
        form_layout.addRow("Item:", self.edit_item)

        # Campo Description
        self.edit_description = QtWidgets.QLineEdit(record.get("Description", ""))
        form_layout.addRow("Descripción:", self.edit_description)

        # Campo Warehouse (puede ser de solo lectura, si la lógica lo requiere)
        self.edit_warehouse = QtWidgets.QLineEdit(record.get("Warehouse", ""))
        # Si se quiere bloquear la edición: self.edit_warehouse.setReadOnly(True)
        form_layout.addRow("Warehouse:", self.edit_warehouse)

        # Campo Location
        self.edit_location = QtWidgets.QLineEdit(record.get("Location", ""))
        form_layout.addRow("Location:", self.edit_location)

        # Campo Ifecha (fecha de expiración) con QDateTimeEdit
        self.edit_ifecha = QtWidgets.QDateTimeEdit(self)
        self.edit_ifecha.setDisplayFormat("dd/MM/yyyy HH:mm:ss")
        self.edit_ifecha.setCalendarPopup(True)
        # Convertir la cadena a QDateTime si es posible
        if record.get("Ifecha"):
            try:
                dt = datetime.strptime(record["Ifecha"], '%d/%m/%Y %H:%M:%S')
                self.edit_ifecha.setDateTime(QtCore.QDateTime(dt))
            except Exception:
                # Si falla la conversión, se deja la fecha actual
                self.edit_ifecha.setDateTime(QtCore.QDateTime.currentDateTime())
        else:
            self.edit_ifecha.setDateTime(QtCore.QDateTime.currentDateTime())
        form_layout.addRow("Fecha:", self.edit_ifecha)

        # Campo On Hand (stock)
        self.spin_onhand = QtWidgets.QSpinBox(self)
        self.spin_onhand.setMaximum(1000000)  # Rango a convenir
        try:
            self.spin_onhand.setValue(int(float(record.get("On Hand", 0))))
        except Exception:
            self.spin_onhand.setValue(0)
        form_layout.addRow("On Hand:", self.spin_onhand)

        # Campo Std (precio o valor estándar)
        self.spin_std = QtWidgets.QDoubleSpinBox(self)
        self.spin_std.setMaximum(1000000.0)
        self.spin_std.setDecimals(2)
        try:
            self.spin_std.setValue(float(record.get("Std", 0.0)))
        except Exception:
            self.spin_std.setValue(0.0)
        form_layout.addRow("Std:", self.spin_std)

        # Campo Status_de_surtido (estado)
        self.combo_status = QtWidgets.QComboBox(self)
        self.combo_status.addItems(["En stock", "Surtido"])
        current_status = record.get("Status_de_surtido", "En stock")
        index = self.combo_status.findText(current_status, QtCore.Qt.MatchFixedString)
        if index >= 0:
            self.combo_status.setCurrentIndex(index)
        form_layout.addRow("Estado:", self.combo_status)

        # Campo Company
        self.edit_company = QtWidgets.QLineEdit(record.get("Company", ""))
        form_layout.addRow("Company:", self.edit_company)

        layout.addLayout(form_layout)

        # Botones de la parte inferior
        button_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def get_updated_record(self):
        """
        Devuelve un diccionario con los datos actualizados a partir de los campos del diálogo.
        Se actualizan únicamente los campos modificables.
        """
        updated = {
            "Skid": self.edit_skid.text().strip(),
            "Item": self.edit_item.text().strip(),
            "Description": self.edit_description.text().strip(),
            "Warehouse": self.edit_warehouse.text().strip(),
            "Location": self.edit_location.text().strip(),
            # Se formatea la fecha al mismo formato que se utiliza en el CSV
            "Ifecha": self.edit_ifecha.dateTime().toString("dd/MM/yyyy HH:mm:ss"),
            "On Hand": self.spin_onhand.value(),
            "Std": self.spin_std.value(),
            "Status_de_surtido": self.combo_status.currentText(),
            "Company": self.edit_company.text().strip()
        }
        return updated


# ==============================================================================
# Clase principal de la pestaña: Window6Page (Métodos Reordenados y Optimizado)
# ==============================================================================
class Window6Page(QtWidgets.QWidget):
    data_updated_signal = QtCore.pyqtSignal()

    # --- Métodos de UI y Slots ---

    def update_alert_status(self, alert_found: bool = False, count: int = 0):
        """Actualiza el estilo y texto del botón de alertas."""
        if not hasattr(self, 'btn_alerta'):
            return

        alert_icon_path = os.path.join(LOCAL_ICON_PATH, "alert_icon.png")
        no_alert_icon_path = os.path.join(LOCAL_ICON_PATH, "bell_icon.png")

        if alert_found:
            style = (
                "QPushButton { background-color:#e74c3c; color:white; "
                "border:1px solid #c0392b; border-radius:5px; padding:8px 12px; "
                "font-size:14px; font-weight:bold; text-align:left; padding-left:10px; } "
                "QPushButton:hover { background-color:#ec7063; } "
                "QPushButton:pressed { background-color:#c0392b; }"
            )
            self.btn_alerta.setText(f" Alertas ({count})")
            self.btn_alerta.setToolTip(f"{count} ítem(s) en alerta.")
            icon = QtGui.QIcon(alert_icon_path) if os.path.exists(alert_icon_path) else QtGui.QIcon()
        else:
            style = (
                "QPushButton { background-color:#5dade2; color:white; border:none; "
                "border-radius:5px; padding:8px 12px; font-size:14px; text-align:left; "
                "padding-left:10px; } QPushButton:hover { background-color:#85c1e9; } "
                "QPushButton:pressed { background-color:#3498db; }"
            )
            self.btn_alerta.setText(" Alertas")
            self.btn_alerta.setToolTip("Verificar ítems (ninguno en alerta).")
            icon = QtGui.QIcon(no_alert_icon_path) if os.path.exists(no_alert_icon_path) else QtGui.QIcon()

        self.btn_alerta.setIcon(icon)
        self.btn_alerta.setStyleSheet(style)
        self.btn_alerta.setIconSize(QtCore.QSize(20, 20))

    def show_expiration_alert_dialog(self):
        """Muestra el diálogo con la lista de ítems en alerta."""
        self.load_expiration_data()  # Recargar datos frescos
        if self.df_expiration.empty or 'Ifecha' not in self.df_expiration.columns:
            QtWidgets.QMessageBox.information(self, "Alertas", "No hay datos cargados.")
            return

        try:
            current_date = datetime.now()
            alert_limit_date = current_date + timedelta(days=EXPIRATION_ALERT_DAYS)
            # Filtrar registros válidos para alerta (verifica Skid, Item, Ifecha, stock y estado)
            expiring_items_df = self.df_expiration[
                (self.df_expiration['Skid'].astype(str).str.strip() != '') &
                (self.df_expiration['Skid'].notna()) &
                (self.df_expiration['Item'].astype(str).str.strip() != '') &
                (self.df_expiration['Item'].notna()) &
                (self.df_expiration['Ifecha'].notna()) &
                (self.df_expiration['Ifecha'] <= alert_limit_date) &
                (self.df_expiration['Status_de_surtido'].fillna("En stock").str.lower() != 'surtido') &
                (self.df_expiration['On Hand'] > 0)
            ].copy()

            print(f"Items filtrados para el diálogo de alerta: {len(expiring_items_df)}")

            if expiring_items_df.empty:
                QtWidgets.QMessageBox.information(self, "Alertas", "¡Buenas noticias! No hay ítems válidos en alerta.")
                self.update_alert_status(False)
                return

            # Aseguramos que la columna Ifecha es datetime y calculamos DaysRemaining
            expiring_items_df['Ifecha'] = pd.to_datetime(expiring_items_df['Ifecha'], errors='coerce')
            expiring_items_df['DaysRemaining'] = (expiring_items_df['Ifecha'] - current_date).dt.days
            expiring_items_df.sort_values(by="DaysRemaining", ascending=True, inplace=True)

            cols_to_show = ["Skid", "Item", "Description", "Ifecha", "DaysRemaining", "Status_de_surtido"]
            missing_cols = [c for c in cols_to_show if c not in expiring_items_df.columns]
            if missing_cols:
                QtWidgets.QMessageBox.critical(self, "Error Datos", f"Faltan cols: {missing_cols}")
                return

            dialog = ExpirationAlertDialog(expiring_items_df[cols_to_show], self)
            dialog.searchRequested.connect(self.handle_alert_search_request)
            dialog.exec_()

        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Error alerta:\n{e}")
            self.check_expiration_alerts()

    def update_surtir_button_state_from_table(self):
        selected_items = self.result_table.selectedItems()
        self.btn_surtir.setEnabled(bool(selected_items))

    def updateCompleter(self):
        """Actualiza las sugerencias para el campo de búsqueda."""
        if not hasattr(self, 'search_field'):
            return

        self.search_field.setCompleter(None)
        suggestions = []
        if hasattr(self, "df_expiration") and not self.df_expiration.empty:
            try:
                if "Skid" in self.df_expiration.columns:
                    suggestions.extend(self.df_expiration["Skid"].dropna().astype(str).unique())
                if "Item" in self.df_expiration.columns:
                    suggestions.extend(self.df_expiration["Item"].dropna().astype(str).unique())
                suggestions = sorted(set(s for s in suggestions if s))
                if suggestions:
                    completer = QtWidgets.QCompleter(suggestions, self.search_field)
                    completer.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
                    completer.setFilterMode(QtCore.Qt.MatchContains)
                    self.search_field.setCompleter(completer)
            except Exception as e:
                print(f"Error completer: {e}")

    def applyFilters(self):
        """Aplica los filtros seleccionados en los ComboBox a la tabla de resultados."""
        filters = [combo.currentText() for combo in self.filter_combo_list]
        for row in range(self.result_table.rowCount()):
            visible = True
            for col in range(self.result_table.columnCount()):
                if filters[col] != "Todos":
                    item = self.result_table.item(row, col)
                    item_text = item.text() if item else ""
                    if filters[col] != item_text:
                        visible = False
                        break
            self.result_table.setRowHidden(row, not visible)

    def search_generic(self):
        """Búsqueda unificada para Skid o Nomenclatura."""
        search_input = self.search_field.text().strip()
        if not search_input:
            QtWidgets.QMessageBox.warning(self, "Aviso", "Ingrese búsqueda.")
            return

        self.last_skid_record = None
        self.btn_surtir.setEnabled(False)
        self.result_table.clearSelection()

        if self.df_expiration.empty:
            QtWidgets.QMessageBox.warning(self, "Aviso", "No hay datos.")
            return

        print(f"Buscando: '{search_input}'")
        match_skid = self.df_expiration[self.df_expiration["Skid"].astype(str).str.strip() == search_input]
        if not match_skid.empty:
            print("Encontrado como SKID.")
            item_code = match_skid.iloc[0].get("Item")
            if item_code and str(item_code).strip():
                self._perform_item_search(item_code, highlight_skid=search_input)
            else:
                QtWidgets.QMessageBox.warning(self, "Datos Incompletos", f"SKID {search_input} sin Item.")
                self.stacked_result.setCurrentIndex(1)
                self.result_table.setRowCount(0)
        else:
            print("No como SKID, buscando como Nomenclatura...")
            self._perform_item_search(search_input)

    def resetSearch(self):
        """Limpia los campos de búsqueda y resultados."""
        self.search_field.clear()
        self.info_panel.clear()
        self.result_table.setRowCount(0)
        for combo in self.filter_combo_list:
            combo.blockSignals(True)
            combo.clear()
            combo.addItem("Todos")
            combo.blockSignals(False)
        self.last_skid_record = None
        self.btn_surtir.setEnabled(False)
        self.check_expiration_alerts()
        self.stacked_result.setCurrentIndex(1)
        self.search_field.setFocus()

    def handle_alert_search_request(self, item_code, skid_to_highlight):
        """Maneja la señal desde el diálogo de alertas para realizar una búsqueda."""
        print(f"Solicitud búsqueda alerta: Item={item_code}, Skid={skid_to_highlight}")
        self.search_field.setText(item_code)
        self._perform_item_search(item_code, highlight_skid=skid_to_highlight)

    def surtir_material_from_table(self):
        """Permite surtir el material del registro seleccionado en la tabla."""
        selected = self.result_table.selectedItems()
        if not selected:
            QtWidgets.QMessageBox.warning(self, "Aviso", "Selecciona fila.")
            return

        row = selected[0].row()
        skid_item = self.result_table.item(row, 0)
        if not skid_item:
            QtWidgets.QMessageBox.critical(self, "Error", "No se pudo obtener Skid.")
            return

        skid = skid_item.text()
        self.load_expiration_data()  # Recargar datos
        if self.df_expiration.empty:
            QtWidgets.QMessageBox.critical(self, "Error", "No datos.")
            return

        record_df = self.df_expiration[self.df_expiration['Skid'] == skid]
        if record_df.empty:
            QtWidgets.QMessageBox.critical(self, "Error", f"SKID {skid} no encontrado.")
            return

        record = record_df.iloc[0].to_dict()
        status = str(record.get("Status_de_surtido", "")).lower()
        if status == "surtido":
            QtWidgets.QMessageBox.warning(self, "Aviso", f"SKID {skid} ya 'Surtido'.")
            return

        try:
            qty_avail = int(float(record.get("On Hand", 0)))
        except Exception:
            QtWidgets.QMessageBox.critical(self, "Error", f"On Hand inválido SKID {skid}.")
            return

        if qty_avail <= 0:
            QtWidgets.QMessageBox.warning(self, "Aviso", f"SKID {skid} sin stock.")
            return

        # --- FEFO Check (Opcional) ---
        qty_surtir = 1
        if qty_avail > 1:
            qty_surtir, ok = QtWidgets.QInputDialog.getInt(
                self, "Cantidad",
                f"<b>SKID:{skid}</b>\nDisp:{qty_avail}\n\n<b>Cantidad:</b>",
                qty_avail, 1, qty_avail
            )
            if not ok:
                print("Surtido cancelado.")
                return
        else:
            reply = QtWidgets.QMessageBox.question(
                self, "Confirmar", f"¿Surtir unidad SKID {skid}?",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                QtWidgets.QMessageBox.Yes
            )
            if reply == QtWidgets.QMessageBox.No:
                return

        print(f"Surtir {qty_surtir} de {qty_avail} SKID {skid}")
        try:
            idx_list = self.df_expiration.index[self.df_expiration["Skid"] == skid].tolist()
            if not idx_list:
                QtWidgets.QMessageBox.critical(self, "Error", f"SKID {skid} no encontrado.")
                return
            idx = idx_list[0]
            curr_qty = int(self.df_expiration.loc[idx, "On Hand"])
            if qty_surtir > curr_qty:
                QtWidgets.QMessageBox.warning(self, "Conflicto", f"Stock cambió a {curr_qty}.")
                self.search_generic()
                return
            rem_qty = curr_qty - qty_surtir
            self.df_expiration.loc[idx, "On Hand"] = rem_qty
            self.df_expiration.loc[idx, "Status_de_surtido"] = "Surtido" if rem_qty == 0 else "En stock"

            if self._save_expiration_data():
                QtWidgets.QMessageBox.information(
                    self, "Éxito",
                    f"<b>{qty_surtir}</b> surtido(s) SKID <b>{skid}</b>.<br>Restante: <b>{rem_qty}</b>"
                )
                self.search_generic()
                self.check_expiration_alerts()
            else:
                self.load_expiration_data()
                self.search_generic()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Error surtido:\n{e}")
            self.load_expiration_data()

    def actualizar_skids(self):
        """Actualiza el archivo de expiraciones a partir de un Excel."""
        filename, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "Seleccionar Excel SKIDs", BASE_NETWORK_PATH, "Excel (*.xlsx *.xls)"
        )
        if not filename:
            return

        print(f"Actualizando desde: {filename}")

        try:
            dtype_spec = {
                'Company': str, 'Item': str, 'Description': str,
                'Wrehouse': str, 'Location': str, 'Skid': str
            }
            df_excel = pd.read_excel(filename, dtype=dtype_spec)
            df_excel.columns = [str(c).strip() for c in df_excel.columns]
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Error leer Excel:\n{e}")
            return

        req_cols = ["Company", "Item", "Description", "Wrehouse", "Location", "On Hand", "Std", "Skid", "Ifecha"]
        missing = [c for c in req_cols if c not in df_excel]
        if missing:
            QtWidgets.QMessageBox.critical(self, "Error", f"Columnas faltantes:\n{missing}")
            return

        whs, loc = "300380", "UBICAR"
        try:
            df_excel["Wrehouse"] = df_excel["Wrehouse"].astype(str).str.strip()
            df_excel["Location"] = df_excel["Location"].astype(str).str.strip().str.upper()
            df_excel["Skid"] = df_excel["Skid"].astype(str).str.split('.').str[0]
            df_filtered = df_excel[
                (df_excel["Wrehouse"] == whs) &
                (df_excel["Location"] == loc) &
                (df_excel["Skid"].str.strip() != "") &
                (df_excel["Skid"].notna())
            ].copy()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Error filtrar Excel:\n{e}")
            return

        print(f"Excel filtrado ({whs}/{loc}): {len(df_filtered)} regs.")
        if df_filtered.empty:
            QtWidgets.QMessageBox.warning(self, "Excel Vacío", f"Excel sin regs para {whs}/{loc}.")
            return

        self.load_expiration_data()
        if self.df_expiration.empty and os.path.exists(EXPIRATION_PATH):
            QtWidgets.QMessageBox.critical(self, "Error", "No datos actuales.")
            return

        df_curr = self.df_expiration.copy()
        cambios = []
        dupl_excel = df_filtered[df_filtered.duplicated(subset=['Skid'], keep=False)]
        if not dupl_excel.empty:
            QtWidgets.QMessageBox.critical(self, "Error", f"SKIDs duplicados Excel: {dupl_excel['Skid'].unique()}")
            return

        df_filtered.set_index('Skid', inplace=True, verify_integrity=False)
        df_curr_idx = df_curr.set_index('Skid', verify_integrity=False)
        skids_excel = set(df_filtered.index)
        cols_comp = ["Company", "Item", "Description", "Warehouse", "Location", "Ifecha", "On Hand", "Std"]

        # Comparar y generar los cambios
        for skid, row in df_filtered.iterrows():
            excel_data = {c: row.get(c) for c in cols_comp}
            excel_data["Ifecha"] = pd.to_datetime(excel_data.get("Ifecha"), errors='coerce')
            excel_data["On Hand"] = int(pd.to_numeric(excel_data.get("On Hand"), errors='coerce').fillna(0))
            excel_data["Std"] = float(pd.to_numeric(excel_data.get("Std"), errors='coerce').fillna(0.0))
            excel_data["Warehouse"], excel_data["Location"] = whs, loc
            curr_data = None
            if skid in df_curr_idx.index:
                matches = df_curr_idx.loc[[skid]]
                curr_data = matches.iloc[0].to_dict() if not matches.empty else None

            if curr_data:
                changed = []
                diff = None
                curr_ifecha = pd.to_datetime(curr_data.get("Ifecha"), errors='coerce')
                curr_onh = int(curr_data.get("On Hand", 0))
                curr_std = float(curr_data.get("Std", 0.0))
                if curr_ifecha != excel_data["Ifecha"]:
                    changed.append("Ifecha")
                if curr_onh != excel_data["On Hand"]:
                    changed.append("On Hand")
                    diff = excel_data["On Hand"] - curr_onh
                if not pd.Index([curr_std]).is_close(excel_data["Std"])[0]:
                    changed.append("Std")
                for c in ["Description", "Company", "Item"]:
                    if curr_data.get(c, "") != excel_data.get(c, ""):
                        changed.append(c)
                curr_stat = str(curr_data.get("Status_de_surtido", "")).lower()
                if changed or curr_stat == 'surtido':
                    if curr_stat == 'surtido':
                        changed.append("Status_de_surtido")
                    reg = {
                        "Skid": skid,
                        "Operation": "Actualizar",
                        "OnHandDiff": diff,
                        "ChangedFields": ",".join(changed),
                        "Status_de_surtido": "En stock"
                    }
                    reg.update(excel_data)
                    cambios.append(reg)
            else:
                reg = {
                    "Skid": skid,
                    "Operation": "Agregar",
                    "OnHandDiff": None,
                    "ChangedFields": "",
                    "Status_de_surtido": "En stock"
                }
                reg.update(excel_data)
                cambios.append(reg)

        df_curr_loc = df_curr[(df_curr["Warehouse"] == whs) & (df_curr["Location"] == loc)]
        skids_loc = set(df_curr_loc['Skid'].astype(str).unique())
        skids_mark = skids_loc - skids_excel
        if skids_mark:
            print(f"SKIDs @ {whs}/{loc} no en Excel: {skids_mark}")
            for skid in skids_mark:
                orig = df_curr[df_curr["Skid"] == skid]
                if not orig.empty:
                    orig_data = orig.iloc[0].to_dict()
                    if str(orig_data.get("Status_de_surtido", "")).lower() != "surtido":
                        diff = -float(orig_data.get("On Hand", 0))
                        reg = {
                            "Skid": skid,
                            "Operation": "Marcar como Surtido",
                            "Item": orig_data.get("Item"),
                            "Description": orig_data.get("Description"),
                            "Ifecha": orig_data.get("Ifecha"),
                            "On Hand": 0,
                            "Status_de_surtido": "Surtido",
                            "OnHandDiff": diff,
                            "ChangedFields": "Status_de_surtido,On Hand",
                            "Company": orig_data.get("Company"),
                            "Warehouse": orig_data.get("Warehouse"),
                            "Location": orig_data.get("Location"),
                            "Std": orig_data.get("Std")
                        }
                        cambios.append(reg)

        if not cambios:
            QtWidgets.QMessageBox.information(self, "Sin Cambios", "No cambios detectados.")
            return

        df_changes = pd.DataFrame(cambios)
        if 'Ifecha' in df_changes:
            df_changes['Ifecha_dt'] = pd.to_datetime(df_changes['Ifecha'], errors='coerce')
            df_changes['Ifecha'] = df_changes['Ifecha_dt'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna("No def")
        cols_ord = ["Operation", "Skid", "Item", "Description", "Warehouse", "Location",
                    "Ifecha", "On Hand", "OnHandDiff", "Std", "Status_de_surtido", "ChangedFields", "Company"]
        df_disp = df_changes[[c for c in cols_ord if c in df_changes]].copy()
        print(f"Cambios propuestos: {len(df_changes)}")
        dialog = ValidationDialog(df_disp, self)
        if dialog.exec_() == QtWidgets.QDialog.Accepted:
            sel_idx = dialog.selected_indices
            if not sel_idx:
                QtWidgets.QMessageBox.information(self, "Cancelado", "No cambios seleccionados.")
                return
            df_sel = df_changes.loc[sel_idx].copy()
            print(f"Aplicando {len(df_sel)} cambios...")
            final_df = self.df_expiration.copy()
            new_rows = []
            for idx, reg in df_sel.iterrows():
                skid = reg["Skid"]
                op = reg["Operation"]
                matches = final_df.index[final_df["Skid"] == skid].tolist()
                if op == "Actualizar":
                    if matches:
                        idx_upd = matches[0]
                        # Actualizar campos definidos en cols_comp
                        for c in cols_comp:
                            if c in reg:
                                final_df.at[idx_upd, c] = reg['Ifecha_dt'] if c == 'Ifecha' and 'Ifecha_dt' in reg else reg[c]
                        final_df.at[idx_upd, "Status_de_surtido"] = "En stock" if ("Status_de_surtido" in reg.get("ChangedFields", "") or final_df.at[idx_upd, "Status_de_surtido"].lower() == 'surtido') else final_df.at[idx_upd, "Status_de_surtido"]
                    else:
                        print(f"WARN: SKID {skid} actualizar no encontrado.")
                elif op == "Agregar":
                    new = {c: reg.get(c) for c in final_df.columns if c in reg}
                    new['Ifecha'] = reg['Ifecha_dt'] if 'Ifecha_dt' in reg else pd.NaT
                    new.setdefault('Status_de_surtido', 'En stock')
                    new.setdefault('On Hand', 0)
                    new_rows.append(new)
                elif op == "Marcar como Surtido":
                    if matches:
                        idx_upd = matches[0]
                        final_df.at[idx_upd, "Status_de_surtido"] = "Surtido"
                        final_df.at[idx_upd, "On Hand"] = 0
                    else:
                        print(f"WARN: SKID {skid} marcar surtido no encontrado.")
            if new_rows:
                final_df = pd.concat([final_df, pd.DataFrame(new_rows).reindex(columns=final_df.columns)], ignore_index=True)
            self.df_expiration = final_df
            if self._save_expiration_data():
                QtWidgets.QMessageBox.information(self, "Éxito", f"Archivo SKIDs actualizado ({len(df_sel)} cambios).")
                self.load_expiration_data()
                self.check_expiration_alerts()
        else:
            QtWidgets.QMessageBox.information(self, "Cancelado", "Actualización cancelada.")

    # --- Métodos de carga, guardado y búsqueda de datos ---

    def load_expiration_data(self):
        """Carga y normaliza el archivo CSV de expiraciones."""
        print(f"Cargando datos: {EXPIRATION_PATH}")
        req = {
            "Skid": "", "Item": "", "Description": "", "Warehouse": "",
            "Location": "", "Ifecha": pd.NaT, "On Hand": 0, "Std": 0.0,
            "Status_de_surtido": "En stock", "Company": ""
        }
        try:
            with self._lock.acquire(timeout=5):
                print("Lock adquirido (read)", EXPIRATION_PATH)
                self.df_expiration = pd.read_csv(
                    EXPIRATION_PATH,
                    encoding='utf-8-sig',
                    converters={'Skid': str, 'Company': str, 'Item': str, 'Warehouse': str, 'Location': str}
                )
                # Función de normalización para limpiar nombres de columnas
                def norm(c): 
                    return c.lstrip('\ufeff').replace("ï»¿", "").strip()
                self.df_expiration.columns = [norm(c) for c in self.df_expiration.columns]
                print("Cols:", list(self.df_expiration.columns))

                # Verificar que todas las columnas requeridas estén presentes
                for c, d in req.items():
                    if c not in self.df_expiration:
                        print(f"WARN: Col '{c}' faltante.")
                        self.df_expiration[c] = d

                # Normalizar y convertir cada columna
                self.df_expiration['Skid'] = self.df_expiration['Skid'].astype(str).str.strip().str.split('.').str[0]
                self.df_expiration['Item'] = self.df_expiration['Item'].astype(str).str.strip()
                self.df_expiration['Ifecha'] = pd.to_datetime(self.df_expiration['Ifecha'], errors='coerce', dayfirst=True)
                self.df_expiration['On Hand'] = pd.to_numeric(self.df_expiration['On Hand'], errors='coerce').fillna(0).astype(int)
                self.df_expiration['Std'] = pd.to_numeric(self.df_expiration['Std'], errors='coerce').fillna(0.0)
                self.df_expiration['Status_de_surtido'] = (
                    self.df_expiration['Status_de_surtido'].fillna("En stock")
                    .astype(str)
                    .str.strip()
                    .replace('', 'En stock')
                )
                for col in ["Description", "Warehouse", "Location", "Company"]:
                    if col in self.df_expiration:
                        self.df_expiration[col] = self.df_expiration[col].astype(str).str.strip()

                print(f"Datos cargados: {len(self.df_expiration)} regs.")
                if hasattr(self, 'search_field'):
                    self.updateCompleter()
            print("Lock liberado (read)", EXPIRATION_PATH)
        except FileLockTimeout:
            QtWidgets.QMessageBox.warning(self, "Lock", f"Timeout lock {os.path.basename(EXPIRATION_PATH)}")
            self.df_expiration = pd.DataFrame()
        except FileNotFoundError:
            QtWidgets.QMessageBox.critical(self, "Error", f"Archivo no encontrado:\n{EXPIRATION_PATH}")
            self.df_expiration = pd.DataFrame(columns=list(req.keys()))
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Error carga:\n{e}")
            print(traceback.format_exc())
            self.df_expiration = pd.DataFrame()

    def _save_expiration_data(self):
        """Guarda los datos actualizados en el archivo CSV."""
        if self.df_expiration is None:
            print("Error: df None.")
            return False
        print(f"Guardando: {EXPIRATION_PATH}")
        try:
            with self._lock.acquire(timeout=5):
                print("Lock adquirido (write)", EXPIRATION_PATH)
                df_save = self.df_expiration.copy()
                if 'Ifecha' in df_save:
                    df_save['Ifecha'] = df_save['Ifecha'].dt.strftime('%d/%m/%Y %H:%M:%S').replace('NaT', '')
                if 'On Hand' in df_save:
                    df_save['On Hand'] = df_save['On Hand'].fillna(0).astype(int)
                if 'Std' in df_save:
                    df_save['Std'] = df_save['Std'].fillna(0.0)

                exp_cols = ["Skid", "Item", "Description", "Warehouse", "Location", "Ifecha", "On Hand", "Std", "Status_de_surtido", "Company"]
                cols_save = [c for c in exp_cols if c in df_save] + [c for c in df_save if c not in exp_cols]
                df_save[cols_save].to_csv(EXPIRATION_PATH, index=False, encoding='utf-8-sig')
                print(f"Archivo {os.path.basename(EXPIRATION_PATH)} guardado.")
                self.data_updated_signal.emit()
                return True
            print("Lock liberado (write)", EXPIRATION_PATH)
        except FileLockTimeout:
            QtWidgets.QMessageBox.critical(self, "Lock", f"Timeout lock write {os.path.basename(EXPIRATION_PATH)}.")
            return False
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Fallo guardado:\n{e}")
            print(traceback.format_exc())
            return False

    def _perform_item_search(self, item_to_search, highlight_skid=None):
        """Realiza la búsqueda y muestra el stock disponible en la tabla."""
        self.stacked_result.setCurrentIndex(1)
        if self.df_expiration.empty or "Item" not in self.df_expiration.columns:
            self.result_table.setRowCount(0)
            return

        matching = self.df_expiration[
            self.df_expiration["Item"].astype(str).str.contains(item_to_search, case=False, na=False)
        ].copy()

        matching = matching[
            (matching["Status_de_surtido"].fillna("En stock").str.lower() != "surtido") &
            (matching["On Hand"] > 0)
        ]
        print(f"Item '{item_to_search}' -> {len(matching)} regs. stock.")

        if matching.empty:
            msg = f"No SKIDs stock para Item '{item_to_search}' (SKID '{highlight_skid}')." if highlight_skid else f"No SKIDs stock para Nomenclatura '{item_to_search}'."
            QtWidgets.QMessageBox.information(self, "Sin Resultados", msg)
            self.result_table.setRowCount(0)
            return

        matching.sort_values(by="Ifecha", ascending=True, inplace=True, na_position='last')
        current_date = datetime.now()
        data = []
        skid_col = "Skid"
        for _, r in matching.iterrows():
            skid = str(r.get(skid_col, "N/A"))
            stock = r.get("On Hand", 0)
            fecha = r.get("Ifecha", pd.NaT)
            f_str = "No def"
            est = "Fecha Indef"
            bg = QtGui.QColor("#e2e3e5")
            diff_sort = float('inf')
            if pd.notnull(fecha):
                f_str = fecha.strftime('%Y-%m-%d')
                diff = (fecha - current_date).days
                diff_sort = diff
                if diff < 0:
                    est, bg = f"Caducado ({abs(diff)}d)", QtGui.QColor("#f8d7da")
                elif diff <= EXPIRATION_ALERT_DAYS:
                    est, bg = f"Próximo ({diff}d)", QtGui.QColor("#fff3cd")
                else:
                    est, bg = f"Vigente ({diff}d)", QtGui.QColor("#d4edda")
            data.append([skid, int(stock), f_str, est, bg, diff_sort])

        self.result_table.setSortingEnabled(False)
        self.result_table.setRowCount(len(data))
        target_row = -1
        for r_idx, r_data in enumerate(data):
            items = [
                QtWidgets.QTableWidgetItem(r_data[0]),
                QtWidgets.QTableWidgetItem(str(r_data[1])),
                QtWidgets.QTableWidgetItem(r_data[2]),
                QtWidgets.QTableWidgetItem(r_data[3])
            ]
            bg = r_data[4]
            if highlight_skid and r_data[0] == highlight_skid:
                target_row = r_idx
                bg = QtGui.QColor("#aed6f1")
            for c_idx, itm in enumerate(items):
                itm.setBackground(bg)
                if c_idx == 1:
                    itm.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                self.result_table.setItem(r_idx, c_idx, itm)
        self.result_table.setSortingEnabled(True)
        self.result_table.sortByColumn(2, QtCore.Qt.AscendingOrder)
        if target_row != -1:
            print(f"Resaltando fila {target_row} SKID {highlight_skid}")
            self.result_table.selectRow(target_row)
            self.result_table.scrollToItem(self.result_table.item(target_row, 0), QtWidgets.QAbstractItemView.PositionAtCenter)
        self.populateFilterOptions()

    def populateFilterOptions(self):
        """Llena los ComboBox de filtro con valores únicos según la tabla actual."""
        if self.result_table.rowCount() == 0:
            for combo in self.filter_combo_list:
                combo.blockSignals(True)
                combo.clear()
                combo.addItem("Todos")
                combo.blockSignals(False)
            return

        for col_idx, combo in enumerate(self.filter_combo_list):
            values = set()
            for row_idx in range(self.result_table.rowCount()):
                item = self.result_table.item(row_idx, col_idx)
                if item:
                    if col_idx == 1:  # Stock
                        try:
                            values.add(int(item.text()))
                        except ValueError:
                            values.add(item.text())
                    else:
                        values.add(item.text())
            combo.blockSignals(True)
            current_selection = combo.currentText()
            combo.clear()
            combo.addItem("Todos")
            try:
                sorted_values = sorted(list(values), key=lambda x: int(x) if isinstance(x, int) else float('inf'))
            except ValueError:
                sorted_values = sorted(str(v) for v in values)
            for val in sorted_values:
                combo.addItem(str(val))
            index = combo.findText(current_selection)
            combo.setCurrentIndex(index if index != -1 else 0)
            combo.blockSignals(False)

    def check_expiration_alerts(self):
        """Verifica las alertas de expiración y actualiza el botón correspondiente."""
        alert_count = 0
        if not self.df_expiration.empty and 'Ifecha' in self.df_expiration.columns:
            try:
                current_date = datetime.now()
                alert_limit_date = current_date + timedelta(days=EXPIRATION_ALERT_DAYS)
                expiring_items = self.df_expiration[
                    (self.df_expiration['Ifecha'].notna()) &
                    (self.df_expiration['Ifecha'] <= alert_limit_date) &
                    (self.df_expiration['Status_de_surtido'].fillna("En stock").str.lower() != 'surtido') &
                    (self.df_expiration['On Hand'] > 0)
                ]
                alert_count = len(expiring_items)
            except Exception as e:
                print(f"Error check alerts: {e}")
        self.update_alert_status(alert_found=(alert_count > 0), count=alert_count)

    def edit_skid(self, item):
        """
        Al hacer doble clic en una fila, se abre un diálogo para editar el registro
        correspondiente, siempre y cuando se cuente con los permisos necesarios.
        """
        if item is None:
            return

        # Obtener el índice de la fila en la tabla
        row = item.row()
        
        # Obtener el SKID de la columna 0
        skid_item = self.result_table.item(row, 0)
        if not skid_item:
            QtWidgets.QMessageBox.critical(self, "Error", "No se pudo obtener el SKID de la fila seleccionada.")
            return

        skid = skid_item.text().strip()
        
        # Validar sesión y permisos de edición, similar a edit_maintenance
        if not Session.user_alias:
            QtWidgets.QMessageBox.critical(self, "Error de sesión",
                                        "La sesión aún no está iniciada. Reinicia la aplicación e inicia sesión.")
            return
        if not check_update_permission(Session.user_alias):
            QtWidgets.QMessageBox.warning(self, "Permisos insuficientes",
                                        "No cuentas con los permisos para editar.")
            return

        print(f"Permisos de edición concedidos a {Session.user_alias} para SKID {skid}")

        # Buscar el registro correspondiente en df_expiration
        record_df = self.df_expiration[self.df_expiration['Skid'] == skid]
        if record_df.empty:
            QtWidgets.QMessageBox.critical(self, "Error", f"SKID {skid} no encontrado en los datos.")
            return

        record = record_df.iloc[0].to_dict()

        # Abrir el diálogo de edición para el registro
        # Se asume que la clase EditSkidDialog existe y se adapta a este uso.
        edit_dialog = EditSkidDialog(record, self)
        if edit_dialog.exec_() == QtWidgets.QDialog.Accepted:
            # Obtener el registro actualizado desde el diálogo
            updated_record = edit_dialog.get_updated_record()
            # Actualizar el DataFrame: se busca el índice del registro a editar.
            idx_list = self.df_expiration.index[self.df_expiration['Skid'] == skid].tolist()
            if not idx_list:
                QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo identificar el registro de SKID {skid}.")
                return
            idx = idx_list[0]
            for key, value in updated_record.items():
                self.df_expiration.at[idx, key] = value
            # Guardar los cambios en el archivo CSV
            if self._save_expiration_data():
                QtWidgets.QMessageBox.information(self, "Éxito", f"SKID {skid} actualizado correctamente.")
                self.search_generic()  # Actualizar la vista de la tabla
                self.check_expiration_alerts()
            else:
                # Si falla la escritura, se recargan los datos.
                self.load_expiration_data()


    # --- Constructor e Inicialización de UI ---

    def __init__(self, parent=None):
        super().__init__(parent)
        self.df_expiration = pd.DataFrame()
        self.last_skid_record = None
        self._lock = FileLock(EXPIRATION_PATH + ".lock", timeout=1)
        self.initUI()
        self.load_expiration_data()
        self.check_expiration_alerts()

    def initUI(self):
        main_layout = QtWidgets.QVBoxLayout(self)
        main_layout.setContentsMargins(15, 15, 15, 15)
        main_layout.setSpacing(10)

        # Header
        header_widget = QtWidgets.QWidget()
        header_layout = QtWidgets.QHBoxLayout(header_widget)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(15)
        header = QtWidgets.QLabel("Control de Fechas de Expiración y Surtido de Consumibles")
        header.setStyleSheet("font-size:20px; font-weight:bold; font-family:'Segoe UI','Century Gothic',sans-serif; color:#2c3e50;")
        header_layout.addWidget(header)
        header_layout.addStretch()

        update_icon = QtGui.QIcon(os.path.join(LOCAL_ICON_PATH, "refresh_icon.png")) if os.path.exists(os.path.join(LOCAL_ICON_PATH, "refresh_icon.png")) else QtGui.QIcon()
        self.btn_actualizar_skid = QtWidgets.QPushButton(update_icon, " Actualizar Skids")
        self.btn_actualizar_skid.setFixedSize(160, 40)
        self.btn_actualizar_skid.setStyleSheet(
            "QPushButton { background-color:#27ae60; color:white; border:none; border-radius:5px; padding:8px 15px; "
            "font-size:14px; font-weight:bold; text-align:left; padding-left:10px; } "
            "QPushButton:hover { background-color:#2ecc71; } "
            "QPushButton:pressed { background-color:#27ae60; }"
        )
        self.btn_actualizar_skid.setIconSize(QtCore.QSize(20, 20))
        self.btn_actualizar_skid.setToolTip("Cargar datos desde Excel.")
        self.btn_actualizar_skid.clicked.connect(self.actualizar_skids)
        header_layout.addWidget(self.btn_actualizar_skid)

        alert_icon = QtGui.QIcon(os.path.join(LOCAL_ICON_PATH, "alert_icon.png")) if os.path.exists(os.path.join(LOCAL_ICON_PATH, "alert_icon.png")) else QtGui.QIcon()
        self.btn_alerta = QtWidgets.QPushButton(alert_icon, " Alertas")
        self.btn_alerta.setFixedSize(130, 40)
        self.btn_alerta.clicked.connect(self.show_expiration_alert_dialog)
        self.btn_alerta.setIconSize(QtCore.QSize(20, 20))
        self.update_alert_status()
        header_layout.addWidget(self.btn_alerta)
        main_layout.addWidget(header_widget)

        # Separador
        separator = QtWidgets.QFrame()
        separator.setFrameShape(QtWidgets.QFrame.HLine)
        separator.setFrameShadow(QtWidgets.QFrame.Sunken)
        main_layout.addWidget(separator)

        # Buscador
        search_groupbox = QtWidgets.QGroupBox("Búsqueda")
        search_groupbox.setStyleSheet("QGroupBox { font-weight:bold; margin-top:5px; } QGroupBox::title { subcontrol-origin:margin; left:10px; padding:0 3px 0 3px; }")
        search_layout = QtWidgets.QHBoxLayout(search_groupbox)
        search_layout.setSpacing(10)
        self.search_field = QtWidgets.QLineEdit()
        self.search_field.setPlaceholderText("Buscar Skid o Nomenclatura...")
        self.search_field.setStyleSheet("QLineEdit { border:1px solid #bdc3c7; border-radius:4px; padding:8px; font-size:14px; }")
        self.search_field.setClearButtonEnabled(True)
        self.search_field.returnPressed.connect(self.search_generic)
        search_layout.addWidget(self.search_field, 1)

        search_icon = QtGui.QIcon(os.path.join(LOCAL_ICON_PATH, "search_icon.png")) if os.path.exists(os.path.join(LOCAL_ICON_PATH, "search_icon.png")) else QtGui.QIcon()
        btn_search = QtWidgets.QPushButton(search_icon, " Buscar")
        btn_search.setStyleSheet(
            "QPushButton { background-color:#3498db; color:white; border:none; border-radius:5px; padding:9px 18px; "
            "font-size:14px; font-weight:bold; } QPushButton:hover { background-color:#5dade2; } "
            "QPushButton:pressed { background-color:#2e86c1; }"
        )
        btn_search.setIconSize(QtCore.QSize(18, 18))
        btn_search.clicked.connect(self.search_generic)
        search_layout.addWidget(btn_search)

        reset_icon = QtGui.QIcon(os.path.join(LOCAL_ICON_PATH, "reset_icon.png")) if os.path.exists(os.path.join(LOCAL_ICON_PATH, "reset_icon.png")) else QtGui.QIcon()
        btn_reset = QtWidgets.QPushButton(reset_icon, " Limpiar")
        btn_reset.setStyleSheet(
            "QPushButton { background-color:#95a5a6; color:white; border:none; border-radius:5px; padding:9px 18px; font-size:14px; } "
            "QPushButton:hover { background-color:#bdc3c7; } QPushButton:pressed { background-color:#7f8c8d; }"
        )
        btn_reset.setIconSize(QtCore.QSize(18, 18))
        btn_reset.clicked.connect(self.resetSearch)
        search_layout.addWidget(btn_reset)

        main_layout.addWidget(search_groupbox)

        # Área de resultados
        results_layout = QtWidgets.QHBoxLayout()
        results_layout.setSpacing(15)
        self.stacked_result = QtWidgets.QStackedWidget()
        self.stacked_result.setStyleSheet("QStackedWidget { border: 1px solid #ddd; border-radius: 5px; }")

        # Panel de información (índice 0)
        self.info_panel = QtWidgets.QTextEdit()
        self.info_panel.setReadOnly(True)
        self.info_panel.setStyleSheet("QTextEdit { font-family:'Segoe UI',sans-serif; font-size:14px; border:none; padding:15px; background-color:#ffffff; }")
        self.stacked_result.addWidget(self.info_panel)

        # Tabla de resultados (índice 1)
        self.table_frame = QtWidgets.QWidget()
        t_layout = QtWidgets.QVBoxLayout(self.table_frame)
        t_layout.setContentsMargins(10, 10, 10, 10)
        t_layout.setSpacing(8)
        filter_groupbox = QtWidgets.QGroupBox("Filtros")
        filter_groupbox.setStyleSheet("QGroupBox { font-weight:bold; margin-top:5px; } QGroupBox::title { subcontrol-origin:margin; left:10px; padding:0 3px 0 3px; }")
        filter_layout = QtWidgets.QHBoxLayout(filter_groupbox)
        self.filter_headers = ["SKID", "Stock", "Fecha de caducidad", "Estado de vigencia"]
        self.filter_combo_list = []
        for header in self.filter_headers:
            vbox = QtWidgets.QVBoxLayout()
            lbl = QtWidgets.QLabel(header)
            lbl.setAlignment(QtCore.Qt.AlignCenter)
            vbox.addWidget(lbl)
            combo = QtWidgets.QComboBox()
            combo.addItem("Todos")
            combo.setStyleSheet("QComboBox { border:1px solid #bdc3c7; border-radius:4px; padding:4px 8px; font-size:12px; min-width:100px; }")
            combo.currentIndexChanged.connect(self.applyFilters)
            self.filter_combo_list.append(combo)
            vbox.addWidget(combo)
            filter_layout.addLayout(vbox)
        t_layout.addWidget(filter_groupbox)

        self.result_table = QtWidgets.QTableWidget()
        self.result_table.setColumnCount(len(self.filter_headers))
        self.result_table.setHorizontalHeaderLabels(self.filter_headers)
        self.result_table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.result_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.result_table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.result_table.verticalHeader().setVisible(False)
        self.result_table.setAlternatingRowColors(True)
        self.result_table.itemDoubleClicked.connect(self.edit_skid)
        self.result_table.setStyleSheet(
            "QTableWidget { gridline-color:#e0e0e0; font-size:13px; } "
            "QHeaderView::section { background-color:#ecf0f1; padding:5px; border:1px solid #dcdcdc; font-weight:bold; } "
            "QTableWidget::item { padding:5px; } QTableWidget::item:selected { background-color:#aed6f1; color:black; }"
        )
        header_view = self.result_table.horizontalHeader()
        header_view.setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        for col in [0, 1, 2, 3]:
            header_view.setSectionResizeMode(col, QtWidgets.QHeaderView.ResizeToContents)
        self.result_table.setSortingEnabled(True)
        t_layout.addWidget(self.result_table)
        self.stacked_result.addWidget(self.table_frame)

        results_layout.addWidget(self.stacked_result, 3)

        # Panel de acciones
        action_panel = QtWidgets.QWidget()
        action_layout = QtWidgets.QVBoxLayout(action_panel)
        action_layout.setContentsMargins(0, 0, 0, 0)
        action_layout.setAlignment(QtCore.Qt.AlignTop)
        surtir_icon = QtGui.QIcon(os.path.join(LOCAL_ICON_PATH, "checkout_icon.png")) if os.path.exists(os.path.join(LOCAL_ICON_PATH, "checkout_icon.png")) else QtGui.QIcon()
        self.btn_surtir = QtWidgets.QPushButton(surtir_icon, " Surtir SKID Seleccionado")
        self.btn_surtir.setMinimumHeight(45)
        self.btn_surtir.setStyleSheet(
            "QPushButton { background-color:#e67e22; color:white; border:none; border-radius:5px; padding:10px 15px; font-size:15px; font-weight:bold; text-align:left; padding-left:10px; } "
            "QPushButton:hover { background-color:#f39c12; } QPushButton:pressed { background-color:#d35400; } "
            "QPushButton:disabled { background-color:#bdc3c7; color:#7f8c8d; }"
        )
        self.btn_surtir.setIconSize(QtCore.QSize(24, 24))
        self.btn_surtir.setToolTip("Marca el SKID seleccionado en la tabla como surtido.")
        self.btn_surtir.clicked.connect(self.surtir_material_from_table)
        self.btn_surtir.setEnabled(False)
        action_layout.addWidget(self.btn_surtir)
        action_layout.addStretch()
        results_layout.addWidget(action_panel, 1)
        main_layout.addLayout(results_layout, 1)

        self.result_table.itemSelectionChanged.connect(self.update_surtir_button_state_from_table)
        self.stacked_result.setCurrentIndex(1)


# ==============================================================================
# Diálogo para Impresión Manual
# ==============================================================================
class ManualPrintDialog(QtWidgets.QDialog):
    def __init__(self, current_printer="", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Impresión Manual")
        self.resize(450, 400)
        layout = QtWidgets.QVBoxLayout(self)

        # Selector de tipo de etiqueta (agregamos FIFO, BIN QR, BIN TEXTO)
        type_layout = QtWidgets.QHBoxLayout()
        type_layout.addWidget(QtWidgets.QLabel("Tipo de etiqueta:"))
        self.combo_type = QtWidgets.QComboBox()
        self.combo_type.addItems(["Identificación", "Mantenimiento", "Identificación Perfiladoras", "Identificación squegees", "FIFO", "BIN QR", "BIN TEXTO","BIN TEXTO CON DESCRIPCION"])
        type_layout.addWidget(self.combo_type)
        layout.addLayout(type_layout)

        # Área de formularios en un QStackedWidget
        self.stack = QtWidgets.QStackedWidget()
        layout.addWidget(self.stack)

        # Página para Identificación
        page_id = QtWidgets.QWidget()
        form_id = QtWidgets.QFormLayout(page_id)
        self.le_proyecto = QtWidgets.QLineEdit()
        self.le_nomenclatura = QtWidgets.QLineEdit()
        self.le_Job = QtWidgets.QLineEdit()
        self.le_Rack = QtWidgets.QLineEdit()
        self.le_prtname_id = QtWidgets.QLineEdit(current_printer)
        self.le_format_id = QtWidgets.QLineEdit(r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_Identificacion.lwl")
        self.le_format_id.setReadOnly(True)
        self.le_llmqty_id = QtWidgets.QLineEdit()
        form_id.addRow("Proyecto:", self.le_proyecto)
        form_id.addRow("Nomenclatura:", self.le_nomenclatura)
        form_id.addRow("Job:", self.le_Job)
        form_id.addRow("Rack:", self.le_Rack)
        form_id.addRow("Impresora:", self.le_prtname_id)
        form_id.addRow("Formato:", self.le_format_id)
        form_id.addRow("Cantidad de etiquetas:", self.le_llmqty_id)
        self.stack.addWidget(page_id)

        # Página para Mantenimiento
        page_maint = QtWidgets.QWidget()
        form_maint = QtWidgets.QFormLayout(page_maint)
        self.le_old_date = QtWidgets.QLineEdit()
        self.le_new_date = QtWidgets.QLineEdit()
        self.le_user_manual = QtWidgets.QLineEdit()
        self.le_prtname_maint = QtWidgets.QLineEdit(current_printer)
        self.le_format_maint = QtWidgets.QLineEdit(r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_Mantenimiento.lwl")
        self.le_format_maint.setReadOnly(True)
        self.le_llmqty_maint = QtWidgets.QLineEdit()
        form_maint.addRow("Fecha actual:", self.le_old_date)
        form_maint.addRow("Fecha futura:", self.le_new_date)
        form_maint.addRow("Usuario:", self.le_user_manual)
        form_maint.addRow("Impresora:", self.le_prtname_maint)
        form_maint.addRow("Formato:", self.le_format_maint)
        form_maint.addRow("Cantidad de etiquetas:", self.le_llmqty_maint)
        self.stack.addWidget(page_maint)

        # Página para Identificación Perfiladoras
        page_id_per = QtWidgets.QWidget()
        form_id_per = QtWidgets.QFormLayout(page_id_per)
        self.le_nomenclatura_per = QtWidgets.QLineEdit()
        self.le_Rack_per = QtWidgets.QLineEdit()
        self.le_prtname_id_per = QtWidgets.QLineEdit(current_printer)
        # Ruta actualizada para la nueva etiqueta.
        self.le_format_id_per = QtWidgets.QLineEdit(r"\\gdlnt104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_Identificacion_Perfiladoras.lwl")
        self.le_format_id_per.setReadOnly(True)
        self.le_llmqty_id_per = QtWidgets.QLineEdit()
        form_id_per.addRow("Nomenclatura:", self.le_nomenclatura_per)
        form_id_per.addRow("Rack:", self.le_Rack_per)
        form_id_per.addRow("Impresora:", self.le_prtname_id_per)
        form_id_per.addRow("Formato:", self.le_format_id_per)
        form_id_per.addRow("Cantidad de etiquetas:", self.le_llmqty_id_per)
        self.stack.addWidget(page_id_per)

        # Página para Identificación squegees
        page_id_sq = QtWidgets.QWidget()
        form_id_sq = QtWidgets.QFormLayout(page_id_sq)
        self.le_proyecto_sq = QtWidgets.QLineEdit()
        self.le_nomenclatura_sq = QtWidgets.QLineEdit()
        self.le_Rack_sq = QtWidgets.QLineEdit()
        self.le_prtname_id_sq = QtWidgets.QLineEdit(current_printer)
        # Ruta actualizada para la nueva etiqueta.
        self.le_format_id_sq = QtWidgets.QLineEdit(r"\\gdlnt104\LABELCONFIG\LABELS\B18\TOOL\ToolTrack+\Recursos\Etiqueta_Identificacion-Squequees.lwl")
        self.le_format_id_sq.setReadOnly(True)
        self.le_llmqty_id_sq = QtWidgets.QLineEdit()
        form_id_sq.addRow("Proyecto:", self.le_proyecto_sq)
        form_id_sq.addRow("Nomenclatura:", self.le_nomenclatura_sq)
        form_id_sq.addRow("Rack:", self.le_Rack_sq)
        form_id_sq.addRow("Impresora:", self.le_prtname_id_sq)
        form_id_sq.addRow("Formato:", self.le_format_id_sq)
        form_id_sq.addRow("Cantidad de etiquetas:", self.le_llmqty_id_sq)
        self.stack.addWidget(page_id_sq)
        # Página para FIFO
        page_fifo = QtWidgets.QWidget()
        form_fifo = QtWidgets.QFormLayout(page_fifo)
        self.le_partnumber = QtWidgets.QLineEdit()
        self.le_descripcion = QtWidgets.QLineEdit()
        self.le_TITULO1 = QtWidgets.QLineEdit()
        self.le_TEXTO1 = QtWidgets.QLineEdit()
        self.le_TITULO2 = QtWidgets.QLineEdit()
        self.le_TEXTO2 = QtWidgets.QLineEdit()
        self.le_TITULO3 = QtWidgets.QLineEdit()
        self.le_TEXTO3 = QtWidgets.QLineEdit()        
        self.le_QTY = QtWidgets.QLineEdit()
        self.le_prtname_fifo = QtWidgets.QLineEdit(current_printer)
        self.le_format_fifo = QtWidgets.QLineEdit(r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_FIFO.lwl")
        self.le_format_fifo.setReadOnly(True)
        self.le_llmqty_fifo = QtWidgets.QLineEdit()
        form_fifo.addRow("PartNumber:", self.le_partnumber)
        form_fifo.addRow("Descripcion:", self.le_descripcion)
        form_fifo.addRow("TITULO 1:", self.le_TITULO1)
        form_fifo.addRow("TEXTO 1:", self.le_TEXTO1)
        form_fifo.addRow("TITULO 2:", self.le_TITULO2)
        form_fifo.addRow("TEXTO 2:", self.le_TEXTO2)
        form_fifo.addRow("TITULO 3:", self.le_TITULO3)
        form_fifo.addRow("TEXTO 3:", self.le_TEXTO3)
        form_fifo.addRow("QTY:", self.le_QTY)
        form_fifo.addRow("Impresora:", self.le_prtname_fifo)
        form_fifo.addRow("Formato:", self.le_format_fifo)
        form_fifo.addRow("Cantidad de etiquetas:", self.le_llmqty_fifo)
        self.stack.addWidget(page_fifo)

        # Página para BIN QR
        page_binqr = QtWidgets.QWidget()
        form_binqr = QtWidgets.QFormLayout(page_binqr)
        self.le_partnumber_binqr = QtWidgets.QLineEdit()
        self.le_descripcion_binqr = QtWidgets.QLineEdit()
        self.le_prtname_binqr = QtWidgets.QLineEdit(current_printer)
        self.le_format_binqr = QtWidgets.QLineEdit(r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_QR.lwl")
        self.le_format_binqr.setReadOnly(True)
        self.le_llmqty_binqr = QtWidgets.QLineEdit("1")
        form_binqr.addRow("PartNumber:", self.le_partnumber_binqr)
        form_binqr.addRow("Descripcion:", self.le_descripcion_binqr)
        form_binqr.addRow("Impresora:", self.le_prtname_binqr)
        form_binqr.addRow("Formato:", self.le_format_binqr)
        form_binqr.addRow("Cantidad de etiquetas:", self.le_llmqty_binqr)
        self.stack.addWidget(page_binqr)

        # Página para BIN TEXTO
        page_bint = QtWidgets.QWidget()
        form_bint = QtWidgets.QFormLayout(page_bint)
        self.le_texto_bint = QtWidgets.QLineEdit()
        self.le_llmqty_bint = QtWidgets.QLineEdit("1")
        self.le_prtname_bint = QtWidgets.QLineEdit(current_printer)
        # Combo para seleccionar tamaño de letra (incluye también número de caracteres)
        self.combo_letra_bint = QtWidgets.QComboBox()
        # Agregamos las opciones; el texto explica el tamaño y el número de caracteres permitido
        self.combo_letra_bint.addItems([
            "7 POINTS (40 CHAR)",
            "8 POINTS (40 CHAR)",
            "9 POINTS (36 CHAR)",
            "10 POINTS (33 CHAR)",
            "11 POINTS (30 CHAR)",
            "12 POINTS (28 CHAR)",
            "13 POINTS (26 CHAR)",
            "14 POINTS (24 CHAR)",
            "15 POINTS (22 CHAR)",
            "16 POINTS (20 CHAR)",
            "17 POINTS (20 CHAR)"            
        ])
        # Campo de formato, se actualizará según la opción seleccionada
        self.le_format_bint = QtWidgets.QLineEdit()
        self.le_format_bint.setReadOnly(True)
        # Diccionario de mapeo para BIN TEXTO
        self.bint_format_map = {
            "7 POINTS (40 CHAR)": r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_TEXTO_7POINTS_40-CHAR.lwl",
            "8 POINTS (40 CHAR)": r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_TEXTO_8POINTS_40-CHAR.lwl",
            "9 POINTS (36 CHAR)": r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_TEXTO_9POINTS_36-CHAR.lwl",
            "10 POINTS (33 CHAR)": r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_TEXTO_10POINTS_33-CHAR.lwl",
            "11 POINTS (30 CHAR)": r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_TEXTO_11POINTS_30-CHAR.lwl",
            "12 POINTS (28 CHAR)": r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_TEXTO_12POINTS_27-CHAR.lwl",
            "13 POINTS (26 CHAR)": r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_TEXTO_13POINTS_26-CHAR.lwl",
            "14 POINTS (24 CHAR)": r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_TEXTO_14POINTS_24-CHAR.lwl",
            "15 POINTS (22 CHAR)": r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_TEXTO_15POINTS_22-CHAR.lwl",
            "16 POINTS (20 CHAR)": r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_TEXTO_16POINTS_20-CHAR.lwl",
            "17 POINTS (20 CHAR)": r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_TEXTO_17POINTS_20-CHAR.lwl"
        }
        # Inicialmente establecemos el formato según la primera opción
        self.le_format_bint.setText(self.bint_format_map[self.combo_letra_bint.currentText()])
        # Conectar la señal para actualizar el formato cuando se cambie la opción
        self.combo_letra_bint.currentIndexChanged.connect(self.update_bint_format)
        form_bint.addRow("TEXTO:", self.le_texto_bint)
        form_bint.addRow("Cantidad de etiquetas:", self.le_llmqty_bint)
        form_bint.addRow("Impresora:", self.le_prtname_bint)
        form_bint.addRow("Tamaño de letra:", self.combo_letra_bint)
        form_bint.addRow("Formato:", self.le_format_bint)
        self.stack.addWidget(page_bint)

        # Página para BIN TEXTO con desc
        page_bintdesc = QtWidgets.QWidget()
        form_bintdesc = QtWidgets.QFormLayout(page_bintdesc)
        self.le_texto_bintdesc = QtWidgets.QLineEdit()
        self.le_desc_bintdesc = QtWidgets.QLineEdit()
        self.le_llmqty_bintdesc = QtWidgets.QLineEdit("1")
        self.le_prtname_bintdesc = QtWidgets.QLineEdit(current_printer)
        # Combo para seleccionar tamaño de letra (incluye también número de caracteres)
        self.combo_letra_bintdesc = QtWidgets.QComboBox()
        # Agregamos las opciones; el texto explica el tamaño y el número de caracteres permitido
        self.combo_letra_bintdesc.addItems([
            "17 POINTS (20 CHAR)"            
        ])
        # Campo de formato, se actualizará según la opción seleccionada
        self.le_format_bintdesc = QtWidgets.QLineEdit()
        self.le_format_bintdesc.setReadOnly(True)
        # Diccionario de mapeo para BIN TEXTO con Descripcion
        self.bintdesc_format_map = {
            "17 POINTS (20 CHAR)": r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_BIN_TEXTO_17POINTS_20-CHAR-DESC-13POINTS_30CHAR.lwl"
        }
        # Inicialmente establecemos el formato según la primera opción
        self.le_format_bintdesc.setText(self.bintdesc_format_map[self.combo_letra_bintdesc.currentText()])
        # Conectar la señal para actualizar el formato cuando se cambie la opción
        self.combo_letra_bintdesc.currentIndexChanged.connect(self.update_bintdesc_format)
        form_bintdesc.addRow("TEXTO:", self.le_texto_bintdesc)
        form_bintdesc.addRow("DESCRIPCION:", self.le_desc_bintdesc)
        form_bintdesc.addRow("Cantidad de etiquetas:", self.le_llmqty_bintdesc)
        form_bintdesc.addRow("Impresora:", self.le_prtname_bintdesc)
        form_bintdesc.addRow("Tamaño de letra:", self.combo_letra_bintdesc)
        form_bintdesc.addRow("Formato:", self.le_format_bintdesc)
        self.stack.addWidget(page_bintdesc)

        # Cambia la página cuando se seleccione el tipo de etiqueta
        self.combo_type.currentIndexChanged.connect(self.stack.setCurrentIndex)

        # Botones de acción
        btn_layout = QtWidgets.QHBoxLayout()
        self.btn_print = QtWidgets.QPushButton("Imprimir")
        self.btn_cancel = QtWidgets.QPushButton("Cancelar")
        btn_layout.addWidget(self.btn_print)
        btn_layout.addWidget(self.btn_cancel)
        layout.addLayout(btn_layout)

        self.btn_cancel.clicked.connect(self.reject)
        self.btn_print.clicked.connect(self.accept)

    def update_bintdesc_format(self):
        # Actualiza el campo de formato para BIN TEXTO según la selección actual
        key = self.combo_letra_bintdesc.currentText()
        if key in self.bintdesc_format_map:
            self.le_format_bintdesc.setText(self.bintdesc_format_map[key])
        else:
            self.le_format_bintdesc.setText("")
    def update_bint_format(self):
        # Actualiza el campo de formato para BIN TEXTO según la selección actual
        key = self.combo_letra_bint.currentText()
        if key in self.bint_format_map:
            self.le_format_bint.setText(self.bint_format_map[key])
        else:
            self.le_format_bint.setText("")

    def getData(self):
        tipo = self.combo_type.currentText()
        if tipo == "Identificación":
            return {
                "tipo": "Identificación",
                "PROYECTO": self.le_proyecto.text().strip(),
                "NOMENCLATURA": self.le_nomenclatura.text().strip(),
                "JOB": self.le_Job.text().strip(),
                "RACK": self.le_Rack.text().strip(),
                "PRTNAME": self.le_prtname_id.text().strip(),
                "FORMAT": self.le_format_id.text().strip(),
                "LLMQTY": self.le_llmqty_id.text().strip()
            }
        elif tipo == "Mantenimiento":
            return {
                "tipo": "Mantenimiento",
                "OLD_DATE": self.le_old_date.text().strip(),
                "NEW_DATE": self.le_new_date.text().strip(),
                "USER": self.le_user_manual.text().strip(),
                "PRTNAME": self.le_prtname_maint.text().strip(),
                "FORMAT": self.le_format_maint.text().strip(),
                "LLMQTY": self.le_llmqty_maint.text().strip()
            }
        elif tipo == "Identificación Perfiladoras":
            return {
                "tipo": "Identificación Perfiladoras",
                "NOMENCLATURA": self.le_nomenclatura_per.text().strip(),
                "RACK": self.le_Rack_per.text().strip(),
                "PRTNAME": self.le_prtname_id_per.text().strip(),
                "FORMAT": self.le_format_id_per.text().strip(),
                "LLMQTY": self.le_llmqty_id_per.text().strip()
            }        
        elif tipo == "Identificación squegees":
            return {
                "tipo": "Identificación squegees",
                "PROYECTO": self.le_proyecto_sq.text().strip(),
                "NOMENCLATURA": self.le_nomenclatura_sq.text().strip(),
                "RACK": self.le_Rack_sq.text().strip(),
                "PRTNAME": self.le_prtname_id_sq.text().strip(),
                "FORMAT": self.le_format_id_sq.text().strip(),
                "LLMQTY": self.le_llmqty_id_sq.text().strip()
            }
        elif tipo == "FIFO":
            return {
                "tipo": "FIFO",
                "PartNumber": self.le_partnumber.text().strip(),
                "Descripcion": self.le_descripcion.text().strip(),
                "TITULO1": self.le_TITULO1.text().strip(),
                "TEXTO1": self.le_TEXTO1.text().strip(),
                "TITULO2": self.le_TITULO2.text().strip(),
                "TEXTO2": self.le_TEXTO2.text().strip(),
                "TITULO3": self.le_TITULO3.text().strip(),
                "TEXTO3": self.le_TEXTO3.text().strip(),
                "QTY": self.le_QTY.text().strip(),
                "PRTNAME": self.le_prtname_fifo.text().strip(),
                "FORMAT": self.le_format_fifo.text().strip(),
                "LLMQTY": self.le_llmqty_fifo.text().strip()
            }
        elif tipo == "BIN QR":
            return {
                "tipo": "BIN QR",
                "PartNumber": self.le_partnumber_binqr.text().strip(),
                "Descripcion": self.le_descripcion_binqr.text().strip(),
                "PRTNAME": self.le_prtname_binqr.text().strip(),
                "FORMAT": self.le_format_binqr.text().strip(),
                "LLMQTY": self.le_llmqty_binqr.text().strip()
            }
        elif tipo == "BIN TEXTO":
            return {
                "tipo": "BIN TEXTO",
                "TEXTO": self.le_texto_bint.text().strip(),
                "LLMQTY": self.le_llmqty_bint.text().strip(),
                "PRTNAME": self.le_prtname_bint.text().strip(),
                "FORMAT": self.le_format_bint.text().strip()
            }
        elif tipo == "BIN TEXTO CON DESCRIPCION":
                        return {
                "tipo": "BIN TEXTO CON DESCRIPCION",
                "TEXTO": self.le_texto_bintdesc.text().strip(),
                "DESC": self.le_desc_bintdesc.text().strip(),
                "LLMQTY": self.le_llmqty_bintdesc.text().strip(),
                "PRTNAME": self.le_prtname_bintdesc.text().strip(),
                "FORMAT": self.le_format_bintdesc.text().strip()
            }
        
# ==============================================================================
# Clase para la Pestaña 7 - Impresión de Etiquetas con autocompletado
# ==============================================================================
class Window7Page(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.initUI()
    
    def initUI(self):
        main_layout = QtWidgets.QVBoxLayout(self)
        
        # Área de Búsqueda (se elimina la selección de tipo para utilizar un solo campo)
        search_layout = QtWidgets.QHBoxLayout()
        self.search_field = QtWidgets.QLineEdit()
        self.search_field.setPlaceholderText("Ingrese criterio de búsqueda (Nomenclatura o Job)...")
        self.search_field.setStyleSheet(
            "background-color: #F0F8FF; margin: 5px; padding: 5px; "
            "border: 2px solid #007ACC; border-radius: 5px;"
        )
        search_layout.addWidget(self.search_field)
        
        # Configuración del autocompletado a partir de la unión de "NOMENCLATURA" y "JOB",
        # excluyendo registros cuyo "TIPO DE HERRAMENTAL" sea "CONSUMABLE"
        self.sugerencias = self.load_sugerencias()
        self.completer = QtWidgets.QCompleter(self.sugerencias, self)
        self.completer.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
        self.completer.setFilterMode(QtCore.Qt.MatchContains)
        self.search_field.setCompleter(self.completer)
        self.search_field.textChanged.connect(self.on_text_changed)
        
        self.btn_search = QtWidgets.QPushButton("Buscar")
        self.btn_search.setToolTip("Inicia la búsqueda con el criterio ingresado.")
        self.btn_search.setStyleSheet(
            "background-color: #FFA07A; color: white; margin: 5px; padding: 5px; border-radius: 5px;"
        )
        self.btn_search.clicked.connect(self.search_item)
        search_layout.addWidget(self.btn_search)
        
        self.btn_refresh = QtWidgets.QPushButton("Refresh")
        self.btn_refresh.setToolTip("Refresca los datos del listado.")
        self.btn_refresh.setStyleSheet(
            "background-color: #007ACC; color: white; margin: 5px; padding: 5px; border-radius: 5px;"
        )
        self.btn_refresh.clicked.connect(self.refresh_data)
        search_layout.addWidget(self.btn_refresh)
        
        main_layout.addLayout(search_layout)
        
        # Tabla de Resultados
        self.results_table = QtWidgets.QTableWidget()
        self.results_table.setColumnCount(5)
        self.results_table.setHorizontalHeaderLabels(["PROYECTO", "NOMENCLATURA", "JOB", "MODELO", "RACK"])
        self.results_table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        main_layout.addWidget(self.results_table)
        
        # Selección de Impresora y Edificio 
        printer_layout = QtWidgets.QHBoxLayout()
        printer_layout.addWidget(QtWidgets.QLabel("Edificio:"))
        self.combo_edificio = QtWidgets.QComboBox()
        edificios = self.load_edificios()
        self.combo_edificio.addItems(edificios)
        self.combo_edificio.currentIndexChanged.connect(self.update_printers)
        printer_layout.addWidget(self.combo_edificio)
        
        printer_layout.addWidget(QtWidgets.QLabel("Impresora:"))
        self.combo_printer = QtWidgets.QComboBox()
        printer_layout.addWidget(self.combo_printer)
        main_layout.addLayout(printer_layout)
        
        # Forzar actualización de impresoras según el edificio seleccionado
        if self.combo_edificio.count() > 0:
            self.combo_edificio.setCurrentIndex(0)
            self.update_printers()
        
        # Botones de Impresión con estilos mejorados
        self.btn_print = QtWidgets.QPushButton("Imprimir etiqueta identificación")
        self.btn_print.setFixedHeight(40)
        self.btn_print.setStyleSheet("""
            QPushButton {
                background-color: #007ACC;
                color: white;
                border: none;
                border-radius: 10px;
                font-size: 14px;
                padding: 10px 20px;
            }
            QPushButton:hover { background-color: #005F9E; }
            QPushButton:pressed { background-color: #003F6F; }
        """)
        self.btn_print.clicked.connect(self.imprimir_etiqueta)
        main_layout.addWidget(self.btn_print)
        
        self.btn_print_maintenance = QtWidgets.QPushButton("Imprimir etiqueta mantenimiento")
        self.btn_print_maintenance.setFixedHeight(40)
        self.btn_print_maintenance.setStyleSheet("""
            QPushButton {
                background-color: #FFA07A;
                color: white;
                border: none;
                border-radius: 10px;
                font-size: 14px;
                padding: 10px 20px;
            }
            QPushButton:hover { background-color: #E07C5D; }
            QPushButton:pressed { background-color: #B85B46; }
        """)
        self.btn_print_maintenance.clicked.connect(self.imprimir_etiqueta_mantenimiento)
        main_layout.addWidget(self.btn_print_maintenance)
        
        self.btn_print_mass = QtWidgets.QPushButton("Impresion masiva")
        self.btn_print_mass.setFixedHeight(40)
        self.btn_print_mass.setStyleSheet("""
            QPushButton {
                background-color: #28A745;
                color: white;
                border: none;
                border-radius: 10px;
                font-size: 14px;
                padding: 10px 20px;
            }
            QPushButton:hover { background-color: #218838; }
            QPushButton:pressed { background-color: #1e7e34; }
        """)
        self.btn_print_mass.clicked.connect(self.abrir_impresion_masiva)
        main_layout.addWidget(self.btn_print_mass)
        
        self.btn_print_manual = QtWidgets.QPushButton("Impresion Manual")
        self.btn_print_manual.setFixedHeight(40)
        self.btn_print_manual.setStyleSheet("""
            QPushButton {
                background-color: #6F42C1;
                color: white;
                border: none;
                border-radius: 10px;
                font-size: 14px;
                padding: 10px 20px;
            }
            QPushButton:hover { background-color: #5936A2; }
            QPushButton:pressed { background-color: #42287F; }
        """)
        self.btn_print_manual.clicked.connect(self.abrir_impresion_manual)
        main_layout.addWidget(self.btn_print_manual)
    
    # Métodos para carga de datos y autocompletado
    def load_sugerencias(self):
        """
        Carga los datos del CSV y retorna una lista única combinando
        valores de "NOMENCLATURA" y "JOB", excluyendo aquellos registros donde
        "TIPO DE HERRAMENTAL" sea "CONSUMABLE".
        """
        try:
            DB_PATH = r"\\gdlnt104\ScanDirs\B18\ToolTrack+\Recursos\DB\MANTENIMIENTO_HERRAMENTALES_TOOLTRACK+.csv"
            df = pd.read_csv(DB_PATH, encoding="utf-8-sig")
            # Excluir registros donde TIPO DE HERRAMENTAL sea "CONSUMABLE"
            df = df[df["TIPO DE HERRAMENTAL"].str.strip().str.upper() != "CONSUMABLE"]
            
            nomen_list = df["NOMENCLATURA"].dropna().unique().tolist() if "NOMENCLATURA" in df.columns else []
            job_list = df["JOB"].dropna().unique().tolist() if "JOB" in df.columns else []
            
            union_list = list(set(nomen_list + job_list))
            union_list.sort()  # Orden alfabético opcional
            return union_list
        except Exception as e:
            print("Error cargando sugerencias:", e)
            return []
    
    def on_text_changed(self, text):
        prefix = text.replace("*", "")
        self.completer.setCompletionPrefix(prefix)
        self.completer.complete()
    
    def load_edificios(self):
        try:
            df = pd.read_excel(r"\\gdlnt104\ScanDirs\B18\ToolTrack+\Recursos\DB\PRINTERS_TOOLTRACK.XLSX")
            df["EDIFICIO"] = df["EDIFICIO"].astype(str).str.strip()
            return df["EDIFICIO"].unique().tolist()
        except Exception as e:
            print("Error cargando edificios:", e)
            return []
    
    def update_printers(self):
        edificio = self.combo_edificio.currentText().strip()
        try:
            df = pd.read_excel(r"\\gdlnt104\ScanDirs\B18\ToolTrack+\Recursos\DB\PRINTERS_TOOLTRACK.XLSX")
            df["EDIFICIO"] = df["EDIFICIO"].astype(str).str.strip()
            df_filtrado = df[df["EDIFICIO"] == edificio]
            self.combo_printer.clear()
            if df_filtrado.empty:
                print("No se encontraron impresoras para el edificio:", edificio)
            else:
                printers = df_filtrado["PRTNAME"].tolist()
                self.combo_printer.addItems(printers)
        except Exception as e:
            print("Error actualizando impresoras:", e)
    
    def search_item(self):
        criterio = self.search_field.text().strip()
        try:
            DB_PATH = r"\\gdlnt104\ScanDirs\B18\ToolTrack+\Recursos\DB\MANTENIMIENTO_HERRAMENTALES_TOOLTRACK+.csv"
            df = pd.read_csv(DB_PATH, encoding="utf-8-sig")
            
            # Excluir registros cuyo "TIPO DE HERRAMENTAL" sea "CONSUMABLE"
            df = df[df["TIPO DE HERRAMENTAL"].str.strip().str.upper() != "CONSUMABLE"]
            
            # Buscar en ambas columnas (NOMENCLATURA y JOB) sin depender de un combo de selección
            df_filtrado = df[
                df["NOMENCLATURA"].str.contains(criterio, case=False, na=False) |
                df["JOB"].str.contains(criterio, case=False, na=False)
            ]
            
            self.populate_results_table(df_filtrado)
        except Exception as e:
            print("Error en búsqueda:", e)

    def populate_results_table(self, df):
        self.results_table.setRowCount(0)
        for idx, row in df.iterrows():
            row_position = self.results_table.rowCount()
            self.results_table.insertRow(row_position)
            item_proyecto = QtWidgets.QTableWidgetItem(str(row["PROYECTO"]))
            item_proyecto.setData(QtCore.Qt.UserRole, row.to_dict())
            self.results_table.setItem(row_position, 0, item_proyecto)
            self.results_table.setItem(row_position, 1, QtWidgets.QTableWidgetItem(str(row["NOMENCLATURA"])))
            self.results_table.setItem(row_position, 2, QtWidgets.QTableWidgetItem(str(row["JOB"])))
            self.results_table.setItem(row_position, 3, QtWidgets.QTableWidgetItem(str(row["MODELO"])))
            self.results_table.setItem(row_position, 4, QtWidgets.QTableWidgetItem(str(row["RACK"])))

    def refresh_data(self):
        self.search_field.clear()
        self.results_table.setRowCount(0)
        print("Datos refrescados.")
    
    def get_target_path(self, impresora):
        try:
            df = pd.read_excel(r"\\gdlnt104\ScanDirs\B18\ToolTrack+\Recursos\DB\PRINTERS_TOOLTRACK.XLSX")
            print("Columnas leídas de XLSX:", df.columns.tolist())
            df["PRTNAME"] = df["PRTNAME"].astype(str).str.strip()
            target_row = df[df["PRTNAME"] == impresora.strip()]
            if not target_row.empty:
                path = target_row.iloc[0]["TARGET_PATH"]
                print("Para impresora", impresora, "se obtuvo TARGET_PATH:", path)
                return path
            else:
                print("No se encontró TARGET_PATH para impresora:", impresora)
                return ""
        except Exception as e:
            print("Error obteniendo TARGET_PATH:", e)
            return ""
    
    def imprimir_etiqueta(self):
        selected_items = self.results_table.selectedItems()
        if not selected_items:
            print("Por favor selecciona un ítem para imprimir la etiqueta.")
            return
        row = selected_items[0].row()
        row_data = self.results_table.item(row, 0).data(QtCore.Qt.UserRole)
        if not row_data:
            print("Datos del ítem incompletos.")
            return
        proyecto = row_data.get("PROYECTO", "")
        nomenclatura = row_data.get("NOMENCLATURA", "")
        job = row_data.get("JOB", "")
        rack = row_data.get("RACK", "")
        formato_path = r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_Identificacion.lwl"
        impresora = self.combo_printer.currentText().strip()
        if impresora.upper() == "NO DEFINIDO":
            QtWidgets.QMessageBox.warning(self, "Advertencia", "Impresora por definir para este edificio")
            return
        target_path = self.get_target_path(impresora)
        if not target_path or str(target_path).strip().upper() in ["", "NAN"]:
            QtWidgets.QMessageBox.warning(self, "Advertencia", "Impresora por definir para este edificio")
            return
        datos_csv = {
            "FORMAT": [formato_path],
            "PRTNAME": [impresora],
            "PROYECTO": [proyecto],
            "NOMENCLATURA": [nomenclatura],
            "JOB": [job],
            "RACK": [rack]

        }
        df_csv = pd.DataFrame(datos_csv)
        safe_nomenclatura = nomenclatura.replace("/", "-")
        csv_file = os.path.join(target_path, f"{safe_nomenclatura}_etiqueta.csv")
        try:
            df_csv.to_csv(csv_file, index=False)
            print("Archivo de etiqueta generado en:", csv_file)
            user = Session.user_alias
            movimiento = "Impresion etiqueta identificacion"
            write_history(user, movimiento, nomenclatura)
        except Exception as e:
            print("Error al generar archivo CSV:", e)
    
    def imprimir_etiqueta_mantenimiento(self):
        selected_items = self.results_table.selectedItems()
        if not selected_items:
            print("Por favor selecciona un ítem para imprimir la etiqueta de mantenimiento.")
            return
        row = selected_items[0].row()
        row_data = self.results_table.item(row, 0).data(QtCore.Qt.UserRole)
        if not row_data:
            print("Datos del ítem incompletos.")
            return
        nomenclatura = row_data.get("NOMENCLATURA", "")
        old_date = row_data.get("ULTIMO_MANTENIMIENTO", "")
        new_date = row_data.get("PROXIMO_MANTENIMIENTO", "")
        user_maintenance = row_data.get("USER_LAST_MAINTENANCE", "")
        formato_path = r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_Mantenimiento.lwl"
        impresora = self.combo_printer.currentText().strip()
        if impresora.upper() == "NO DEFINIDO":
            QtWidgets.QMessageBox.warning(self, "Advertencia", "Impresora por definir para este edificio")
            return
        target_path = self.get_target_path(impresora)
        if not target_path or str(target_path).strip().upper() in ["", "NAN"]:
            QtWidgets.QMessageBox.warning(self, "Advertencia", "Impresora por definir para este edificio")
            return
        datos_csv = {
            "FORMAT": [formato_path],
            "PRTNAME": [impresora],
            "OLD_DATE": [old_date],
            "NEW_DATE": [new_date],
            "USER": [user_maintenance]
        }
        df_csv = pd.DataFrame(datos_csv)
        safe_nomenclatura = nomenclatura.replace("/", "-")
        csv_file = os.path.join(target_path, f"{safe_nomenclatura}_mantenimiento.csv")
        try:
            df_csv.to_csv(csv_file, index=False)
            print("Archivo de etiqueta de mantenimiento generado en:", csv_file)
            movimiento = "Impresion etiqueta mantenimiento"
            write_history(user_maintenance, movimiento, nomenclatura)
        except Exception as e:
            print("Error al generar archivo CSV de mantenimiento:", e)
    
    def abrir_impresion_masiva(self):
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle("Impresión Masiva")
        layout = QtWidgets.QVBoxLayout(dialog)
        tipo_layout = QtWidgets.QHBoxLayout()
        tipo_layout.addWidget(QtWidgets.QLabel("Tipo de etiqueta:"))
        self.combo_etiqueta_mass = QtWidgets.QComboBox()
        self.combo_etiqueta_mass.addItems(["Identificación", "Mantenimiento"])
        tipo_layout.addWidget(self.combo_etiqueta_mass)
        layout.addLayout(tipo_layout)
        list_widget = QtWidgets.QListWidget()
        list_widget.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        DB_PATH = r"\\gdlnt104\ScanDirs\B18\ToolTrack+\Recursos\DB\MANTENIMIENTO_HERRAMENTALES_TOOLTRACK+.csv"
        try:
            df = pd.read_csv(DB_PATH, encoding="utf-8-sig")
        except Exception as e:
            print("Error al cargar la base de datos:", e)
            return
        for idx, row in df.iterrows():
            item_text = f"{row['NOMENCLATURA']} - {row['PROYECTO']} - {row['JOB']} - {row['RACK']}"
            item = QtWidgets.QListWidgetItem(item_text)
            item.setData(QtCore.Qt.UserRole, row.to_dict())
            list_widget.addItem(item)
        layout.addWidget(list_widget)
        btn_print_selected = QtWidgets.QPushButton("Imprimir Seleccionados")
        btn_print_selected.clicked.connect(lambda: self.imprimir_masiva(dialog, list_widget))
        layout.addWidget(btn_print_selected)
        dialog.exec_()
    
    def imprimir_masiva(self, dialog, list_widget):
        selected_items = list_widget.selectedItems()
        if not selected_items:
            print("No hay ítems seleccionados.")
            return
        tipo_etiqueta = self.combo_etiqueta_mass.currentText()
        impresora = self.combo_printer.currentText().strip()
        target_path = self.get_target_path(impresora)
        if not target_path or str(target_path).strip().upper() in ["", "NAN"]:
            print("Error: No se encontró TARGET_PATH para la impresora", impresora)
            return
        rows = []
        for item in selected_items:
            data = item.data(QtCore.Qt.UserRole)
            if tipo_etiqueta == "Identificación":
                proyecto = data.get("PROYECTO", "")
                nomenclatura = data.get("NOMENCLATURA", "")
                job = data.get("JOB", "")
                rack = data.get("RACK", "")

                formato_path = r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_Identificacion.lwl"
                row_dict = {
                    "FORMAT": formato_path,
                    "PRTNAME": impresora,
                    "PROYECTO": proyecto,
                    "NOMENCLATURA": nomenclatura,
                    "JOB": job,
                    "RACK": rack
                }
                movimiento = "Impresion masiva (identificacion)"
                write_history(Session.user_alias, movimiento, nomenclatura)
            else:
                nomenclatura = data.get("NOMENCLATURA", "")
                old_date = data.get("ULTIMO_MANTENIMIENTO", "")
                new_date = data.get("PROXIMO_MANTENIMIENTO", "")
                user_maintenance = data.get("USER_LAST_MAINTENANCE", "")
                formato_path = r"\\GDLNT104\ScanDirs\B18\ToolTrack+\Recursos\Etiqueta_Mantenimiento.lwl"
                row_dict = {
                    "FORMAT": formato_path,
                    "PRTNAME": impresora,
                    "OLD_DATE": old_date,
                    "NEW_DATE": new_date,
                    "USER": user_maintenance
                }
                movimiento = "Impresion masiva (mantenimiento)"
                write_history(user_maintenance, movimiento, nomenclatura)
            rows.append(row_dict)
        df_csv = pd.DataFrame(rows)
        base_filename = "Tooltrack+_etiqueta_Identificacion_manual.csv" if tipo_etiqueta == "Identificación" else "Tooltrack+_etiqueta_Mantenimiento_manual.csv"
        csv_file = os.path.join(target_path, base_filename)
        try:
            df_csv.to_csv(csv_file, index=False)
            print("Archivo generado en:", csv_file)
        except Exception as e:
            print(f"Error al generar archivo CSV para impresión masiva: {e}")
        dialog.accept()
    
    def abrir_impresion_manual(self):
        current_printer = self.combo_printer.currentText().strip()
        dialog = ManualPrintDialog(current_printer, self)
        if dialog.exec_() == QtWidgets.QDialog.Accepted:
            data = dialog.getData()
            if data["tipo"] == "Identificación":
                datos_csv = {
                    "FORMAT": [data["FORMAT"]],
                    "PRTNAME": [data["PRTNAME"]],
                    "PROYECTO": [data["PROYECTO"]],
                    "NOMENCLATURA": [data["NOMENCLATURA"]],
                    "JOB": [data["JOB"]],
                    "RACK": [data["RACK"]],
                    "LLMQTY": [data["LLMQTY"]]
                }
                base_filename = "Tooltrack+_etiqueta_Identificacion_manual.csv"
                movimiento = "Impresion manual (identificacion)"
                write_history(Session.user_alias, movimiento, data["NOMENCLATURA"])
            elif data["tipo"] == "Mantenimiento":
                datos_csv = {
                    "FORMAT": [data["FORMAT"]],
                    "PRTNAME": [data["PRTNAME"]],
                    "OLD_DATE": [data["OLD_DATE"]],
                    "NEW_DATE": [data["NEW_DATE"]],
                    "USER": [data["USER"]],
                    "LLMQTY": [data["LLMQTY"]]
                }
                base_filename = "Tooltrack+_etiqueta_Mantenimiento_manual.csv"
                movimiento = "Impresion manual (mantenimiento)"
                write_history(Session.user_alias, movimiento)
            elif data["tipo"] == "Identificación Perfiladoras":
                datos_csv = {
                    "FORMAT": [data["FORMAT"]],
                    "PRTNAME": [data["PRTNAME"]],
                    "NOMENCLATURA": [data["NOMENCLATURA"]],
                    "RACK": [data["RACK"]],
                    "LLMQTY": [data["LLMQTY"]]
                }
                base_filename = "Tooltrack+_etiqueta_Identificacion_Perfiladoras_manual.csv"
                movimiento = "Impresion manual (Identificacion Perfiladoras)"
                write_history(Session.user_alias, movimiento, data["NOMENCLATURA"])
            elif data["tipo"] == "Identificación squegees":
                datos_csv = {
                    "FORMAT": [data["FORMAT"]],
                    "PRTNAME": [data["PRTNAME"]],
                    "PROYECTO": [data["PROYECTO"]],
                    "NOMENCLATURA": [data["NOMENCLATURA"]],
                    "RACK": [data["RACK"]],
                    "LLMQTY": [data["LLMQTY"]]
                }
                base_filename = "Tooltrack+_etiqueta_Identificacion_squegees_manual.csv"
                movimiento = "Impresion manual (identificacion squegees)"
                write_history(Session.user_alias, movimiento, data["NOMENCLATURA"])
            elif data["tipo"] == "FIFO":
                datos_csv = {
                    "FORMAT": [data["FORMAT"]],
                    "PRTNAME": [data["PRTNAME"]],
                    "PartNumber": [data["PartNumber"]],
                    "Descripcion": [data["Descripcion"]],
                    "TITULO1": [data["TITULO1"]],
                    "TEXTO1": [data["TEXTO1"]],
                    "TITULO2": [data["TITULO2"]],
                    "TEXTO2": [data["TEXTO2"]],
                    "TITULO3": [data["TITULO3"]],
                    "TEXTO3": [data["TEXTO3"]],
                    "QTY": [data["QTY"]],
                    "LLMQTY": [data["LLMQTY"]]
                }
                base_filename = "Tooltrack+_etiqueta_FIFO_manual.csv"
                movimiento = "Impresion manual (FIFO)"
                # Usamos el PartNumber para el historial
                write_history(Session.user_alias, movimiento, data.get("PartNumber", ""))
            elif data["tipo"] == "BIN QR":
                datos_csv = {
                    "FORMAT": [data["FORMAT"]],
                    "PRTNAME": [data["PRTNAME"]],
                    "PartNumber": [data["PartNumber"]],
                    "Descripcion": [data["Descripcion"]],
                    "LLMQTY": [data["LLMQTY"]]
                }
                base_filename = "Tooltrack+_etiqueta_BIN_QR_manual.csv"
                movimiento = "Impresion manual (BIN QR)"
                write_history(Session.user_alias, movimiento, data.get("PartNumber", ""))
            elif data["tipo"] == "BIN TEXTO":
                datos_csv = {
                    "FORMAT": [data["FORMAT"]],
                    "PRTNAME": [data["PRTNAME"]],
                    "TEXTO": [data["TEXTO"]],
                    "LLMQTY": [data["LLMQTY"]]
                }
                base_filename = "Tooltrack+_etiqueta_BIN_TEXTO_manual.csv"
                movimiento = "Impresion manual (BIN TEXTO)"
                write_history(Session.user_alias, movimiento, data.get("TEXTO", ""))
            elif data["tipo"] == "BIN TEXTO CON DESCRIPCION":
                datos_csv = {
                    "FORMAT": [data["FORMAT"]],
                    "PRTNAME": [data["PRTNAME"]],
                    "TEXTO": [data["TEXTO"]],
                    "DESC": [data["DESC"]],
                    "LLMQTY": [data["LLMQTY"]]
                }
                base_filename = "Tooltrack+_etiqueta_BIN_TEXTO_manual.csv"
                movimiento = "Impresion manual (BIN TEXTO CON DESC)"
                write_history(Session.user_alias, movimiento, data.get("TEXTO", ""))

            
            target_path = self.get_target_path(data["PRTNAME"])
            if not target_path or str(target_path).strip().upper() in ["", "NAN"]:
                QtWidgets.QMessageBox.warning(self, "Advertencia", "Impresora por definir para este edificio")
                return
            df_csv = pd.DataFrame(datos_csv)
            csv_file = os.path.join(target_path, base_filename)
            try:
                df_csv.to_csv(csv_file, index=False)
                print("Archivo generado (Manual) en:", csv_file)
            except Exception as e:
                print("Error al generar archivo CSV manual:", e)
                
# -----------------------------------------------------------------------------
# Clase AnimatedButton
# -----------------------------------------------------------------------------
class AnimatedButton(QtWidgets.QPushButton):
    def __init__(self, *args, **kwargs):
        """
        Se crea un botón que posee un widget overlay (sin afectar el layout)
        que se animará para dar feedback visual sin modificar el sizeHint.
        """
        super().__init__(*args, **kwargs)
        # Creamos el overlay; este widget se coloca sobre el contenido del botón.
        self.overlay = QtWidgets.QWidget(self)
        # Se le asigna un fondo negro totalmente transparente
        self.overlay.setStyleSheet("background-color: rgba(0, 0, 0, 0);")
        # El overlay no debe interferir en los eventos de mouse
        self.overlay.setAttribute(QtCore.Qt.WA_TransparentForMouseEvents)
        self.overlay.setGeometry(self.rect())
        self.overlay.raise_()
        # Animación sobre la propiedad windowOpacity del overlay
        self.anim = QtCore.QPropertyAnimation(self.overlay, b"windowOpacity", self)
        self.anim.setDuration(150)
        self.anim.setStartValue(0)
        self.anim.setKeyValueAt(0.5, 0.3)
        self.anim.setEndValue(0)
    
    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.overlay.setGeometry(self.rect())
    
    def animate_click(self):
        self.anim.stop()
        # Iniciamos la animación sin el flag DeleteWhenStopped para evitar que se elimine.
        self.anim.start()

# -----------------------------------------------------------------------------
# Clase de botón personalizado para presets (usa AnimatedButton)
# -----------------------------------------------------------------------------
class CustomPresetButton(AnimatedButton):
    def __init__(self, preset_number, parent=None):
        super().__init__(f"Personalizado {preset_number}", parent)
        self.preset_number = preset_number
        self.setMinimumHeight(50)
        # Vista previa por defecto; se actualizará al cargar el preset
        self.update_preview(["#d99227", "#f0f0f0", "#fcf6d7"])
    
    def update_preview(self, colors):
        """
        Dibuja un QPixmap dividido en tres partes con los colores indicados
        y lo asigna como icono. Esto sirve para mostrar una vista previa de los
        colores almacenados en el preset.
        """
        width = 60
        height = 20
        pixmap = QtGui.QPixmap(width, height)
        pixmap.fill(QtCore.Qt.transparent)
        painter = QtGui.QPainter(pixmap)
        third = width // 3
        for i, color in enumerate(colors):
            rect = QtCore.QRect(i * third, 0, third, height)
            painter.fillRect(rect, QtGui.QColor(color))
        painter.end()
        self.setIcon(QtGui.QIcon(pixmap))
        self.setIconSize(QtCore.QSize(width, height))

class AddUserDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Añadir Usuario")
        self.setModal(True)
        self.setStyleSheet(GENERAL_STYLESHEET)
        self.resize(800, 600)  # Cambiar el tamaño inicial del diálogo
        self.initUI()
    
    def initUI(self):
        layout = QtWidgets.QVBoxLayout(self)
        
        # Formulario de campos obligatorios
        form_layout = QtWidgets.QFormLayout()
        
        self.edit_alias = QtWidgets.QLineEdit()
        form_layout.addRow("Alias/gdl *", self.edit_alias)
        
        self.edit_nomina = QtWidgets.QLineEdit()
        form_layout.addRow("Nómina *", self.edit_nomina)
        
        self.edit_correo = QtWidgets.QLineEdit()
        form_layout.addRow("Correo *", self.edit_correo)
        
        self.edit_name = QtWidgets.QLineEdit()
        form_layout.addRow("Nombre *", self.edit_name)
        
        self.edit_lastname = QtWidgets.QLineEdit()
        form_layout.addRow("Apellido *", self.edit_lastname)
        
        self.combo_update_object = QtWidgets.QComboBox()
        self.combo_update_object.addItems(["YES", "NO"])
        form_layout.addRow("Actualizar Objeto *", self.combo_update_object)
        
        layout.addLayout(form_layout)
        
        # Grupo de privilegios por módulos con nombres descriptivos
        group_box = QtWidgets.QGroupBox("Privilegios de Módulos")
        group_layout = QtWidgets.QGridLayout()
        modules = [
            ("MODULE_1", "Inventario"),
            ("MODULE_2", "Factor de Uso"),
            ("MODULE_3", "Mantenimiento"),
            ("MODULE_4", "Entrada/Salida"),
            ("MODULE_5", "Expiración"),
            ("MODULE_6", "Imprimir"),
            ("MODULE_7", "Configuración"),
            ("MODULE_8", "Historial")
        ]
        self.module_checkboxes = {}
        for i, (mod_key, mod_label) in enumerate(modules):
            cb = QtWidgets.QCheckBox(mod_label)
            self.module_checkboxes[mod_key] = cb
            row = i // 2
            col = i % 2
            group_layout.addWidget(cb, row, col)
        group_box.setLayout(group_layout)
        layout.addWidget(group_box)
        
        # Botones para Aceptar y Cancelar
        btn_layout = QtWidgets.QHBoxLayout()
        btn_layout.addStretch()
        
        self.btn_accept = QtWidgets.QPushButton("Agregar Usuario")
        self.btn_accept.setStyleSheet(BTN_STYLE_ACCEPT)
        self.btn_accept.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_accept)
        
        self.btn_cancel = QtWidgets.QPushButton("Cancelar")
        self.btn_cancel.setStyleSheet(BTN_STYLE_REJECT)
        self.btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(self.btn_cancel)
        
        layout.addLayout(btn_layout)
    
    def get_data(self):
        # Obtener los datos obligatorios y de privilegios
        alias = self.edit_alias.text().strip()
        nomina = self.edit_nomina.text().strip()
        correo = self.edit_correo.text().strip()
        name = self.edit_name.text().strip()
        lastname = self.edit_lastname.text().strip()
        update_object = self.combo_update_object.currentText().strip()
        
        # Validar que se hayan completado todos los campos obligatorios
        if not (alias and nomina and correo and name and lastname):
            QtWidgets.QMessageBox.critical(self, "Error", "Complete todos los campos obligatorios.")
            return None
        
        modules_values = { key: "true" if cb.isChecked() else "0" 
                           for key, cb in self.module_checkboxes.items() }
        
        # Valores predeterminados para los campos CFG
        defaults = {
            "CFG_HEADER_COLOR": "#d99227",
            "CFG_SIDEBAR_COLOR": "#f0f0f0",
            "CFG_FRAME_COLOR": "#fcf6d7",
            "CFG_OPACITY": "100",
            "CFG_HEADER_OPACITY": "100",
            "CFG_SIDEBAR_OPACITY": "100",
            "CFG_FRAME_OPACITY": "100",
            "CFG_CUSTOM1_HEADER_COLOR": "#2d2d2d",
            "CFG_CUSTOM1_SIDEBAR_COLOR": "#a498fe",
            "CFG_CUSTOM1_FRAME_COLOR": "#c6c2f3",
            "CFG_CUSTOM1_HEADER_OPACITY": "100",
            "CFG_CUSTOM1_SIDEBAR_OPACITY": "100",
            "CFG_CUSTOM1_FRAME_OPACITY": "100",
            "CFG_CUSTOM2_HEADER_COLOR": "#181818",
            "CFG_CUSTOM2_SIDEBAR_COLOR": "#ce9172",
            "CFG_CUSTOM2_FRAME_COLOR": "#7cdcfe",
            "CFG_CUSTOM2_HEADER_OPACITY": "100",
            "CFG_CUSTOM2_SIDEBAR_OPACITY": "100",
            "CFG_CUSTOM2_FRAME_OPACITY": "100",
            "CFG_CUSTOM3_HEADER_COLOR": "#96c73d",
            "CFG_CUSTOM3_SIDEBAR_COLOR": "#a2c464",
            "CFG_CUSTOM3_FRAME_COLOR": "#ffdb75",
            "CFG_CUSTOM3_HEADER_OPACITY": "100",
            "CFG_CUSTOM3_SIDEBAR_OPACITY": "100",
            "CFG_CUSTOM3_FRAME_OPACITY": "100",
        }
        
        new_user = {
            "ALIAS": alias,
            "NOMINA": nomina,
            "CORREO": correo,
            "NAME": name,
            "LASTNAME": lastname,
            "STATUS": "ACTIVE",  # Por defecto
            "UPDATE_OBJECT": update_object,
        }
        new_user.update(modules_values)
        new_user.update(defaults)
        return new_user
        
class ModifyUserDialog(QtWidgets.QDialog):
    """
    Diálogo para modificar ciertos campos del usuario seleccionado.
    El diálogo carga todos los alias desde USERS_DB_PATH; al seleccionar uno,
    se muestran los campos modificables:
      - Datos Textuales: Nómina, Correo, Nombre y Apellido.
      - UPDATE_OBJECT (ComboBox con YES/NO)
      - STATUS (ComboBox con ACTIVE/INACTIVE)
      - Privilegios para módulos (MODULE_1 a MODULE_8) mediante checkboxes.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Modificar Usuario")
        self.setModal(True)
        self.setStyleSheet(GENERAL_STYLESHEET)
        self.resize(800, 600)  # Cambiar el tamaño inicial del diálogo
        self.df_users = None  # DataFrame con todos los usuarios.
        self.initUI()
        self.load_users()
    
    def initUI(self):
        layout = QtWidgets.QVBoxLayout(self)
        
        # --- Seleccionar Usuario ---
        select_layout = QtWidgets.QFormLayout()
        self.combo_users = QtWidgets.QComboBox()
        self.combo_users.currentTextChanged.connect(self.on_user_selection)
        select_layout.addRow("Seleccione Usuario:", self.combo_users)
        layout.addLayout(select_layout)
        
        # --- Formulario de campos modificables ---
        form_layout = QtWidgets.QFormLayout()
        
        # Campos textuales para edición de datos básicos
        self.edit_nomina = QtWidgets.QLineEdit()
        form_layout.addRow("Nómina *", self.edit_nomina)
        
        self.edit_correo = QtWidgets.QLineEdit()
        form_layout.addRow("Correo *", self.edit_correo)
        
        self.edit_name = QtWidgets.QLineEdit()
        form_layout.addRow("Nombre *", self.edit_name)
        
        self.edit_lastname = QtWidgets.QLineEdit()
        form_layout.addRow("Apellido *", self.edit_lastname)
        
        # Actualización de objeto
        self.combo_update_object = QtWidgets.QComboBox()
        self.combo_update_object.addItems(["YES", "NO"])
        form_layout.addRow("Actualizar Objeto:", self.combo_update_object)
        
        # Estado
        self.combo_status = QtWidgets.QComboBox()
        self.combo_status.addItems(["ACTIVE", "INACTIVE"])
        form_layout.addRow("Estado:", self.combo_status)
        
        layout.addLayout(form_layout)
        
        # --- Privilegios de módulos mediante Checkboxes ---
        group_box = QtWidgets.QGroupBox("Privilegios de Módulos")
        grid = QtWidgets.QGridLayout()
        # Lista de módulos y sus etiquetas descriptivas
        modules = [
            ("MODULE_1", "Inventario"),
            ("MODULE_2", "Factor de Uso"),
            ("MODULE_3", "Mantenimiento"),
            ("MODULE_4", "Entrada/Salida"),
            ("MODULE_5", "Expiración"),
            ("MODULE_6", "Imprimir"),
            ("MODULE_7", "Configuración"),
            ("MODULE_8", "Historial")
        ]
        self.check_modules = {}
        for i, (mod, label) in enumerate(modules):
            cb = QtWidgets.QCheckBox(label)
            self.check_modules[mod] = cb
            row = i // 2
            col = i % 2
            grid.addWidget(cb, row, col)
        group_box.setLayout(grid)
        layout.addWidget(group_box)
        
        # --- Botones de acción ---
        btn_layout = QtWidgets.QHBoxLayout()
        btn_layout.addStretch()
        self.btn_accept = QtWidgets.QPushButton("Guardar cambios")
        self.btn_accept.setStyleSheet(BTN_STYLE_ACCEPT)
        self.btn_accept.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_accept)
        
        self.btn_cancel = QtWidgets.QPushButton("Cancelar")
        self.btn_cancel.setStyleSheet(BTN_STYLE_REJECT)
        self.btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(self.btn_cancel)
        layout.addLayout(btn_layout)
    
    def load_users(self):
        """Carga el CSV de usuarios y rellena el combo box con los alias."""
        try:
            self.df_users = pd.read_csv(USERS_DB_PATH, encoding="utf-8-sig")
            # Normalizamos las columnas a mayúsculas
            self.df_users.columns = [col.upper() for col in self.df_users.columns]
            aliases = self.df_users["ALIAS"].dropna().unique().tolist()
            aliases = [alias.strip() for alias in aliases]
            self.combo_users.clear()
            self.combo_users.addItems(aliases)
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"No se pudieron cargar los usuarios:\n{e}")
    
    def on_user_selection(self, alias):
        """Cuando se selecciona un usuario, carga sus datos en el formulario."""
        if not alias or self.df_users is None:
            return
        record = self.df_users[self.df_users["ALIAS"].str.strip().str.lower() == alias.strip().lower()]
        if record.empty:
            return
        rec = record.iloc[0]
        # Cargar los campos textuales
        self.edit_nomina.setText(str(rec.get("NOMINA", "")))
        self.edit_correo.setText(str(rec.get("CORREO", "")))
        self.edit_name.setText(str(rec.get("NAME", "")))
        self.edit_lastname.setText(str(rec.get("LASTNAME", "")))
        # UPDATE_OBJECT
        current_update = str(rec.get("UPDATE_OBJECT", "NO")).strip().upper()
        index = self.combo_update_object.findText(current_update)
        self.combo_update_object.setCurrentIndex(index if index >= 0 else 0)
        # STATUS
        current_status = str(rec.get("STATUS", "ACTIVE")).strip().upper()
        index = self.combo_status.findText(current_status)
        self.combo_status.setCurrentIndex(index if index >= 0 else 0)
        # Privilegios: marcar según el valor en el registro
        for mod, cb in self.check_modules.items():
            val = str(rec.get(mod, "0")).strip().lower()
            cb.setChecked(val in ["true", "1"])
    
    def get_data(self):
        """
        Retorna un diccionario con los nuevos valores para los campos modificados
        para el usuario seleccionado, por ejemplo:
          {
            "ALIAS": <alias seleccionado>,
            "NOMINA": <valor>,
            "CORREO": <valor>,
            "NAME": <valor>,
            "LASTNAME": <valor>,
            "UPDATE_OBJECT": <valor>,
            "STATUS": <valor>,
            "MODULE_1": <"true" o "0">,
            ...,
            "MODULE_8": <"true" o "0">
          }
        """
        selected_alias = self.combo_users.currentText().strip()
        data = {
            "ALIAS": selected_alias,
            "NOMINA": self.edit_nomina.text().strip(),
            "CORREO": self.edit_correo.text().strip(),
            "NAME": self.edit_name.text().strip(),
            "LASTNAME": self.edit_lastname.text().strip(),
            "UPDATE_OBJECT": self.combo_update_object.currentText().strip(),
            "STATUS": self.combo_status.currentText().strip()
        }
        for mod, cb in self.check_modules.items():
            data[mod] = "true" if cb.isChecked() else "0"
        return data
        
# -----------------------------------------------------------------------------
# Clase Window8Page: Configuración/Personalización
# -----------------------------------------------------------------------------
class Window8Page(QtWidgets.QWidget):
    # Se emitirá esta señal (con colores y opacidades actuales) para que la
    # ventana principal actualice sus estilos.
    config_updated = pyqtSignal(str, str, str, int, int, int)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.initUI()
        self.load_current_config()
    
    def initUI(self):
        layout = QtWidgets.QVBoxLayout(self)
        
        # Título
        title = QtWidgets.QLabel("Configuración y Personalización")
        title.setAlignment(QtCore.Qt.AlignCenter)
        title.setStyleSheet("font: bold 14pt 'Montserrat';")
        layout.addWidget(title)
        
        # --- Formulario de configuración ---
        form_layout = QtWidgets.QFormLayout()
        
        # Color Encabezado
        self.header_color_edit = QtWidgets.QLineEdit()
        self.header_color_edit.setPlaceholderText("#d99227")
        btn_header_color = AnimatedButton("Elegir")
        btn_header_color.clicked.connect(lambda _: self.choose_color(self.header_color_edit))
        self.set_button_style(btn_header_color, base_color="#d99227")
        hlayout_header = QtWidgets.QHBoxLayout()
        hlayout_header.addWidget(self.header_color_edit)
        hlayout_header.addWidget(btn_header_color)
        form_layout.addRow("Color Encabezado:", hlayout_header)
        
        # Opacidad Encabezado
        self.header_opacity_slider = QtWidgets.QSlider(QtCore.Qt.Horizontal)
        self.header_opacity_slider.setRange(50, 100)
        self.header_opacity_slider.setValue(100)
        form_layout.addRow("Opacidad Encabezado (%):", self.header_opacity_slider)
        
        # Color Barra Lateral
        self.sidebar_color_edit = QtWidgets.QLineEdit()
        self.sidebar_color_edit.setPlaceholderText("#f0f0f0")
        btn_sidebar_color = AnimatedButton("Elegir")
        btn_sidebar_color.clicked.connect(lambda _: self.choose_color(self.sidebar_color_edit))
        self.set_button_style(btn_sidebar_color, base_color="#f0f0f0", text_color="#333")
        hlayout_sidebar = QtWidgets.QHBoxLayout()
        hlayout_sidebar.addWidget(self.sidebar_color_edit)
        hlayout_sidebar.addWidget(btn_sidebar_color)
        form_layout.addRow("Color Barra Lateral:", hlayout_sidebar)
        
        # Opacidad Barra Lateral
        self.sidebar_opacity_slider = QtWidgets.QSlider(QtCore.Qt.Horizontal)
        self.sidebar_opacity_slider.setRange(50, 100)
        self.sidebar_opacity_slider.setValue(100)
        form_layout.addRow("Opacidad Barra Lateral (%):", self.sidebar_opacity_slider)
        
        # Color Fondo Contenido
        self.frame_color_edit = QtWidgets.QLineEdit()
        self.frame_color_edit.setPlaceholderText("#fcf6d7")
        btn_frame_color = AnimatedButton("Elegir")
        btn_frame_color.clicked.connect(lambda _: self.choose_color(self.frame_color_edit))
        self.set_button_style(btn_frame_color, base_color="#fcf6d7")
        hlayout_frame = QtWidgets.QHBoxLayout()
        hlayout_frame.addWidget(self.frame_color_edit)
        hlayout_frame.addWidget(btn_frame_color)
        form_layout.addRow("Color Fondo Contenido:", hlayout_frame)
        
        # Opacidad Fondo Contenido
        self.frame_opacity_slider = QtWidgets.QSlider(QtCore.Qt.Horizontal)
        self.frame_opacity_slider.setRange(50, 100)
        self.frame_opacity_slider.setValue(100)
        form_layout.addRow("Opacidad Fondo Contenido (%):", self.frame_opacity_slider)
        
        layout.addLayout(form_layout)
        
        # --- Presets Predefinidos ---
        presets_group = QtWidgets.QGroupBox("Preconfiguraciones de Color")
        presets_layout = QtWidgets.QHBoxLayout()
        
        btn_original = AnimatedButton("Original")
        self.set_button_style(btn_original, base_color="#d99227")
        btn_original.clicked.connect(lambda _, btn=btn_original: (btn.animate_click(), self.set_original_colors()))
        
        btn_claro = AnimatedButton("Modo Claro")
        self.set_button_style(btn_claro, base_color="#f0f0f0", text_color="#333")
        btn_claro.clicked.connect(lambda _, btn=btn_claro: (btn.animate_click(), self.set_light_colors()))
        
        btn_oscuro = AnimatedButton("Modo Oscuro")
        self.set_button_style(btn_oscuro, base_color="#2d2d2d")
        btn_oscuro.clicked.connect(lambda _, btn=btn_oscuro: (btn.animate_click(), self.set_dark_colors()))
        
        btn_moderno = AnimatedButton("Modo Moderno")
        self.set_button_style(btn_moderno, base_color="#3a506b")
        btn_moderno.clicked.connect(lambda _, btn=btn_moderno: (btn.animate_click(), self.set_modern_colors()))
        
        presets_layout.addWidget(btn_original)
        presets_layout.addWidget(btn_claro)
        presets_layout.addWidget(btn_oscuro)
        presets_layout.addWidget(btn_moderno)
        presets_group.setLayout(presets_layout)
        layout.addWidget(presets_group)
        
        # --- Presets Personalizados ---
        custom_group = QtWidgets.QGroupBox("Personalizados")
        custom_layout = QtWidgets.QHBoxLayout()
        self.custom_buttons = {}
        for i in range(1, 5):
            btn_custom = CustomPresetButton(i)
            self.set_button_style(btn_custom, base_color="#808080")
            btn_custom.clicked.connect(
                lambda checked, preset=i, btn=btn_custom: (btn.animate_click(), self.load_custom_preset(preset))
            )
            self.custom_buttons[i] = btn_custom
            custom_layout.addWidget(btn_custom)
        custom_group.setLayout(custom_layout)
        layout.addWidget(custom_group)
        
        # --- Botones de acción ---
        btn_save = AnimatedButton("Guardar Configuración")
        self.set_button_style(btn_save, base_color="#007ACC")
        btn_save.clicked.connect(lambda _, btn=btn_save: (btn.animate_click(), self.save_config()))
        layout.addWidget(btn_save)
        
        btn_save_custom = AnimatedButton("Guardar como Personalizado")
        self.set_button_style(btn_save_custom, base_color="#007ACC")
        btn_save_custom.clicked.connect(lambda _, btn=btn_save_custom: (btn.animate_click(), self.save_as_custom_preset()))
        layout.addWidget(btn_save_custom)
        
        btn_refresh = AnimatedButton("Refresh")
        self.set_button_style(btn_refresh, base_color="#007ACC")
        btn_refresh.clicked.connect(lambda _, btn=btn_refresh: (btn.animate_click(), self.reload_config()))
        layout.addWidget(btn_refresh)

        btn_add_user = AnimatedButton("Añadir Usuario")
        self.set_button_style(btn_add_user, base_color="#007ACC")
        btn_add_user.clicked.connect(lambda _, btn=btn_add_user: (btn.animate_click(), self.agregar_usuario()))
        layout.addWidget(btn_add_user)

        btn_mod_user = AnimatedButton("Modificar Usuario")
        self.set_button_style(btn_mod_user, base_color="#007ACC")
        btn_mod_user.clicked.connect(lambda _, btn=btn_mod_user: (btn.animate_click(), self.modificar_usuario()))
        layout.addWidget(btn_mod_user)

   
    def agregar_usuario(self):
        dialog = AddUserDialog(self)
        if dialog.exec_() == QtWidgets.QDialog.Accepted:
            user_data = dialog.get_data()
            if user_data is None:
                return
            try:
                # Cargar el CSV existente o crear uno nuevo con las columnas fijas:
                columns = [
                    "USER_ID","ALIAS","NOMINA","CORREO","NAME","LASTNAME","STATUS","UPDATE_OBJECT",
                    "CFG_HEADER_COLOR","CFG_SIDEBAR_COLOR","CFG_FRAME_COLOR","CFG_OPACITY",
                    "CFG_HEADER_OPACITY","CFG_SIDEBAR_OPACITY","CFG_FRAME_OPACITY",
                    "CFG_CUSTOM1_HEADER_COLOR","CFG_CUSTOM1_SIDEBAR_COLOR","CFG_CUSTOM1_FRAME_COLOR",
                    "CFG_CUSTOM1_HEADER_OPACITY","CFG_CUSTOM1_SIDEBAR_OPACITY","CFG_CUSTOM1_FRAME_OPACITY",
                    "CFG_CUSTOM2_HEADER_COLOR","CFG_CUSTOM2_SIDEBAR_COLOR","CFG_CUSTOM2_FRAME_COLOR",
                    "CFG_CUSTOM2_HEADER_OPACITY","CFG_CUSTOM2_SIDEBAR_OPACITY","CFG_CUSTOM2_FRAME_OPACITY",
                    "CFG_CUSTOM3_HEADER_COLOR","CFG_CUSTOM3_SIDEBAR_COLOR","CFG_CUSTOM3_FRAME_COLOR",
                    "CFG_CUSTOM3_HEADER_OPACITY","CFG_CUSTOM3_SIDEBAR_OPACITY","CFG_CUSTOM3_FRAME_OPACITY",
                    "MODULE_1","MODULE_2","MODULE_3","MODULE_4","MODULE_5","MODULE_6","MODULE_7","MODULE_8"
                ]
                if os.path.isfile(USERS_DB_PATH):
                    df = pd.read_csv(USERS_DB_PATH, encoding="utf-8-sig")
                    # Si el archivo existe, asegurarse de tener todas las columnas
                    for col in columns:
                        if col not in df.columns:
                            df[col] = ""
                    # Se pueden eliminar filas duplicadas si es necesario, por ejemplo, usando ALIAS único
                else:
                    df = pd.DataFrame(columns=columns)
                
                # Convertir la columna ALIAS a minúsculas para comparación insensible:
                if not df.empty:
                    if df["ALIAS"].str.lower().eq(user_data["ALIAS"].strip().lower()).any():
                        QtWidgets.QMessageBox.information(self, "Duplicado",
                            "El usuario con ese alias ya existe.\nInicie el diálogo de modificación.")
                        # Se podría invocar un diálogo de modificación aquí, en este ejemplo se retorna.
                        return
                
                # Asignar un nuevo USER_ID:
                # Si el DataFrame está vacío se asigna 1,
                # de lo contrario, se convierte la columna a numérico reemplazando NaN por 0 y se asigna el máximo + 1.
                if df.empty:
                    new_id = 1
                else:
                    df["USER_ID"] = pd.to_numeric(df["USER_ID"], errors="coerce").fillna(0)
                    new_id = int(df["USER_ID"].max() + 1)
                user_data["USER_ID"] = new_id
                
                # Convertir el diccionario de datos a DataFrame y concatenarlo:
                new_row = pd.DataFrame([user_data])
                df = pd.concat([df, new_row], ignore_index=True)
                df.to_csv(USERS_DB_PATH, index=False, encoding="utf-8-sig")
                QtWidgets.QMessageBox.information(self, "Éxito", "Usuario agregado correctamente.")
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error", f"Error al guardar el usuario:\n{e}")

    def modificar_usuario(self):
        # Validar que el usuario logueado tenga privilegios de modificación
        if not check_update_permission(Session.user_alias):
            QtWidgets.QMessageBox.warning(self, "Permisos insuficientes",
                                        "No tienes privilegios para modificar usuarios.")
            return

        dlg = ModifyUserDialog(self)
        if dlg.exec_() == QtWidgets.QDialog.Accepted:
            updated_data = dlg.get_data()
            try:
                df = pd.read_csv(USERS_DB_PATH, encoding="utf-8-sig")
                df.columns = [col.upper() for col in df.columns]
                alias = updated_data["ALIAS"].strip().lower()
                for key, value in updated_data.items():
                    if key != "ALIAS":
                        df.loc[df["ALIAS"].str.strip().str.lower() == alias, key] = value
                df.to_csv(USERS_DB_PATH, index=False, encoding="utf-8-sig")
                QtWidgets.QMessageBox.information(self, "Éxito", "El usuario se modificó correctamente.")
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error", f"Error al actualizar el usuario:\n{e}")
        else:
            QtWidgets.QMessageBox.information(self, "Modificación cancelada", "No se realizaron cambios.")

    def set_button_style(self, button, base_color, text_color="white"):
        """
        Aplica estilos definidos vía QSS al botón.
        Se calculan colores hover y pressed a partir del color base.
        """
        hover_color = self.adjust_color(base_color, 20)
        pressed_color = self.adjust_color(base_color, -20)
        style = f"""
            AnimatedButton {{
                background-color: {base_color};
                color: {text_color};
                border: none;
                border-radius: 5px;
                padding: 5px 10px;
            }}
            AnimatedButton:hover {{
                background-color: {hover_color};
            }}
            AnimatedButton:pressed {{
                background-color: {pressed_color};
            }}
        """
        button.setStyleSheet(style)
    
    def choose_color(self, line_edit):
        color = QtWidgets.QColorDialog.getColor()
        if color.isValid():
            line_edit.setText(color.name())
    
    def load_current_config(self):
        try:
            users_df = pd.read_csv(USERS_DB_PATH, encoding="utf-8-sig")
            # Buscar la fila del usuario (ignorando mayúsculas/minúsculas)
            user_row = users_df[users_df["ALIAS"].str.lower() == Session.user_alias.lower()].iloc[0]
            
            # Función auxiliar para obtener valores de texto de forma segura
            def safe_get(key, default):
                value = user_row.get(key, default)
                # Si el valor es NaN o no es una cadena, se devuelve el valor por defecto
                if pd.isna(value) or not isinstance(value, str):
                    return default
                return value
            
            # Función auxiliar para obtener valores numéricos de forma segura
            def safe_get_numeric(key, default):
                value = user_row.get(key, default)
                if pd.isna(value):
                    return default
                try:
                    return int(float(value))
                except Exception:
                    return default
            
            self.header_color_edit.setText(safe_get("CFG_HEADER_COLOR", "#d99227"))
            self.sidebar_color_edit.setText(safe_get("CFG_SIDEBAR_COLOR", "#f0f0f0"))
            self.frame_color_edit.setText(safe_get("CFG_FRAME_COLOR", "#fcf6d7"))
            
            self.header_opacity_slider.setValue(safe_get_numeric("CFG_HEADER_OPACITY", 100))
            self.sidebar_opacity_slider.setValue(safe_get_numeric("CFG_SIDEBAR_OPACITY", 100))
            self.frame_opacity_slider.setValue(safe_get_numeric("CFG_FRAME_OPACITY", 100))
            
            for i in range(1, 5):
                custom_prefix = f"CFG_CUSTOM{i}_"
                header = safe_get(custom_prefix + "HEADER_COLOR", "#d99227")
                sidebar = safe_get(custom_prefix + "SIDEBAR_COLOR", "#f0f0f0")
                frame = safe_get(custom_prefix + "FRAME_COLOR", "#fcf6d7")
                self.custom_buttons[i].update_preview([header, sidebar, frame])
                
        except Exception as e:
            print("Error cargando configuración:", e)
            self.header_color_edit.setText("#d99227")
            self.sidebar_color_edit.setText("#f0f0f0")
            self.frame_color_edit.setText("#fcf6d7")
            self.header_opacity_slider.setValue(100)
            self.sidebar_opacity_slider.setValue(100)
            self.frame_opacity_slider.setValue(100)
        
    def save_config(self):
        try:
            users_df = pd.read_csv(USERS_DB_PATH, encoding="utf-8-sig")
            alias = Session.user_alias
            idx = users_df[users_df["ALIAS"].str.lower() == alias.lower()].index[0]
            
            users_df.at[idx, "CFG_HEADER_COLOR"] = self.header_color_edit.text().strip() or "#d99227"
            users_df.at[idx, "CFG_SIDEBAR_COLOR"] = self.sidebar_color_edit.text().strip() or "#f0f0f0"
            users_df.at[idx, "CFG_FRAME_COLOR"] = self.frame_color_edit.text().strip() or "#fcf6d7"
            
            users_df.at[idx, "CFG_HEADER_OPACITY"] = self.header_opacity_slider.value()
            users_df.at[idx, "CFG_SIDEBAR_OPACITY"] = self.sidebar_opacity_slider.value()
            users_df.at[idx, "CFG_FRAME_OPACITY"] = self.frame_opacity_slider.value()
            
            users_df.to_csv(USERS_DB_PATH, index=False, encoding="utf-8-sig")
            QtWidgets.QMessageBox.information(self, "Éxito", "¡Configuración guardada!")
            
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Error guardando configuración:\n{str(e)}")
    
    def save_as_custom_preset(self):
        preset, ok = QtWidgets.QInputDialog.getInt(
            self,
            "Guardar como Personalizado",
            "Ingrese el número de preset al que desea guardar (1-4):",
            min=1,
            max=4
        )
        if ok:
            try:
                users_df = pd.read_csv(USERS_DB_PATH, encoding="utf-8-sig")
                alias = Session.user_alias
                idx = users_df[users_df["ALIAS"].str.lower() == alias.lower()].index[0]
                
                custom_prefix = f"CFG_CUSTOM{preset}_"
                users_df.at[idx, custom_prefix + "HEADER_COLOR"] = self.header_color_edit.text().strip() or "#d99227"
                users_df.at[idx, custom_prefix + "SIDEBAR_COLOR"] = self.sidebar_color_edit.text().strip() or "#f0f0f0"
                users_df.at[idx, custom_prefix + "FRAME_COLOR"] = self.frame_color_edit.text().strip() or "#fcf6d7"
                users_df.at[idx, custom_prefix + "HEADER_OPACITY"] = self.header_opacity_slider.value()
                users_df.at[idx, custom_prefix + "SIDEBAR_OPACITY"] = self.sidebar_opacity_slider.value()
                users_df.at[idx, custom_prefix + "FRAME_OPACITY"] = self.frame_opacity_slider.value()
                
                users_df.to_csv(USERS_DB_PATH, index=False, encoding="utf-8-sig")
                QtWidgets.QMessageBox.information(self, "Éxito", f"Preset personalizado {preset} guardado!")
                
                self.custom_buttons[preset].update_preview([
                    self.header_color_edit.text().strip() or "#d99227",
                    self.sidebar_color_edit.text().strip() or "#f0f0f0",
                    self.frame_color_edit.text().strip() or "#fcf6d7"
                ])
                
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error", f"Error guardando preset personalizado {preset}:\n{str(e)}")
    
    def load_custom_preset(self, preset):
        try:
            import pandas as pd  # Asegurarse de tener pandas importado
            users_df = pd.read_csv(USERS_DB_PATH, encoding="utf-8-sig")
            # Buscar la fila del usuario (ignorando mayúsculas/minúsculas)
            user_row = users_df[users_df["ALIAS"].str.lower() == Session.user_alias.lower()].iloc[0]
            custom_prefix = f"CFG_CUSTOM{preset}_"
            
            # Obtener valores de colores
            header_color = user_row.get(custom_prefix + "HEADER_COLOR")
            sidebar_color = user_row.get(custom_prefix + "SIDEBAR_COLOR")
            frame_color = user_row.get(custom_prefix + "FRAME_COLOR")
            
            # Obtener valores de opacidad
            header_opacity = user_row.get(custom_prefix + "HEADER_OPACITY")
            sidebar_opacity = user_row.get(custom_prefix + "SIDEBAR_OPACITY")
            frame_opacity = user_row.get(custom_prefix + "FRAME_OPACITY")
            
            # Asignar valores de color, aplicando un valor por defecto si se detecta NaN
            if header_color and not pd.isna(header_color):
                self.header_color_edit.setText(str(header_color))
            else:
                self.header_color_edit.setText("#d99227")
            
            if sidebar_color and not pd.isna(sidebar_color):
                self.sidebar_color_edit.setText(str(sidebar_color))
            else:
                self.sidebar_color_edit.setText("#f0f0f0")
                
            if frame_color and not pd.isna(frame_color):
                self.frame_color_edit.setText(str(frame_color))
            else:
                self.frame_color_edit.setText("#fcf6d7")
            
            # Función auxiliar para configurar la opacidad
            def set_slider_value(slider, value):
                if value is not None and not pd.isna(value):
                    try:
                        slider.setValue(int(float(value)))
                    except Exception:
                        slider.setValue(100)
                else:
                    slider.setValue(100)
            
            set_slider_value(self.header_opacity_slider, header_opacity)
            set_slider_value(self.sidebar_opacity_slider, sidebar_opacity)
            set_slider_value(self.frame_opacity_slider, frame_opacity)
            
        except Exception as e:
            QtWidgets.QMessageBox.warning(
                self,
                "Error",
                f"No se pudo cargar el preset personalizado {preset}:\n{str(e)}"
            )

    def reload_config(self):
        self.load_current_config()
        print("Emitting config_updated signal")  # Debug
        print("Valores:", self.header_color_edit.text(), self.sidebar_color_edit.text())  # Debug
        self.config_updated.emit(
            self.header_color_edit.text().strip() or "#d99227",
            self.sidebar_color_edit.text().strip() or "#f0f0f0",
            self.frame_color_edit.text().strip() or "#fcf6d7",
            self.header_opacity_slider.value(),
            self.sidebar_opacity_slider.value(),
            self.frame_opacity_slider.value()
        )
        QtWidgets.QMessageBox.information(self, "Actualizado", "Configuración recargada.")

    # Métodos para presets predefinidos
    def set_original_colors(self):
        self.header_color_edit.setText("#d99227")
        self.sidebar_color_edit.setText("#f0f0f0")
        self.frame_color_edit.setText("#fcf6d7")
        self.header_opacity_slider.setValue(100)
        self.sidebar_opacity_slider.setValue(100)
        self.frame_opacity_slider.setValue(100)

    def set_light_colors(self):
        self.header_color_edit.setText("#4a90e2")
        self.sidebar_color_edit.setText("#f8f9fa")
        self.frame_color_edit.setText("#ffffff")
        self.header_opacity_slider.setValue(100)
        self.sidebar_opacity_slider.setValue(100)
        self.frame_opacity_slider.setValue(100)

    def set_dark_colors(self):
        self.header_color_edit.setText("#2d2d2d")
        self.sidebar_color_edit.setText("#3d3d3d")
        self.frame_color_edit.setText("#797979")
        self.header_opacity_slider.setValue(100)
        self.sidebar_opacity_slider.setValue(100)
        self.frame_opacity_slider.setValue(100)

    def set_modern_colors(self):
        self.header_color_edit.setText("#3a506b")
        self.sidebar_color_edit.setText("#5bc0be")
        self.frame_color_edit.setText("#ffffff")
        self.header_opacity_slider.setValue(100)
        self.sidebar_opacity_slider.setValue(100)
        self.frame_opacity_slider.setValue(100)

    def adjust_color(self, hex_color, amount):
        rgb = [int(hex_color[i:i+2], 16) for i in (1, 3, 5)]
        new_rgb = [min(255, max(0, c + amount)) for c in rgb]
        return f"#{new_rgb[0]:02x}{new_rgb[1]:02x}{new_rgb[2]:02x}"

# ============================================================================
# Window9Page - Historial
# ============================================================================
class Window9Page(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db_mapping = {}  # Diccionario que mapeará NOMENCLATURA -> JOB
        self.current_export_df = pd.DataFrame()
        self.initUI()
        self.load_db_mapping()  # Cargar mapping desde DB_PATH.
        self.load_history()

    def initUI(self):
        layout = QtWidgets.QVBoxLayout(self)
        
        # Título
        title = QtWidgets.QLabel("Historial de Movimientos")
        title.setAlignment(QtCore.Qt.AlignCenter)
        title.setStyleSheet("font: bold 14pt 'Montserrat';")
        layout.addWidget(title)
        
        # Panel de Filtros
        filtro_widget = QtWidgets.QWidget()
        filtro_layout = QtWidgets.QHBoxLayout(filtro_widget)
        filtro_layout.setSpacing(10)
        
        # Rango de fechas: por defecto desde dos días atrás hasta hoy
        self.date_from = QtWidgets.QDateEdit(calendarPopup=True)
        self.date_from.setDisplayFormat("dd/MM/yy")
        self.date_from.setDate(QtCore.QDate.currentDate().addDays(-2))
        filtro_layout.addWidget(QtWidgets.QLabel("Desde:"))
        filtro_layout.addWidget(self.date_from)
        
        self.date_to = QtWidgets.QDateEdit(calendarPopup=True)
        self.date_to.setDisplayFormat("dd/MM/yy")
        self.date_to.setDate(QtCore.QDate.currentDate())
        filtro_layout.addWidget(QtWidgets.QLabel("Hasta:"))
        filtro_layout.addWidget(self.date_to)
        
        # Filtro por Movimiento
        self.movimiento_combo = QtWidgets.QComboBox()
        filtro_layout.addWidget(QtWidgets.QLabel("Movimiento:"))
        filtro_layout.addWidget(self.movimiento_combo)
        
        # Filtro por NOMENCLATURA/JOB con autocompletado
        self.search_edit = QtWidgets.QLineEdit()
        self.search_edit.setPlaceholderText("Buscar por NOMENCLATURA/JOB")
        filtro_layout.addWidget(self.search_edit)
        # Configuración del completer:
        self.search_completer = QtWidgets.QCompleter()
        self.search_completer.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
        self.search_completer.setFilterMode(QtCore.Qt.MatchContains)
        self.search_edit.setCompleter(self.search_completer)
        
        # Botón aplicar filtros
        btn_filtrar = QtWidgets.QPushButton("Filtrar")
        btn_filtrar.setStyleSheet(STYLE_BUTTON)
        btn_filtrar.clicked.connect(self.apply_filters)
        filtro_layout.addWidget(btn_filtrar)
        
        layout.addWidget(filtro_widget)
        
        # Tabla de Historial
        self.table = QtWidgets.QTableWidget()
        # Creamos 11 columnas con el orden requerido.
        self.table.setColumnCount(11)
        self.table.setHorizontalHeaderLabels([
            "History_ID", "USER", "NOMENCLATURA", "JOB", "LINEA", "USER MFG",
            "ESTADO", "MOVIMIENTO", "COMENTARIO", "QTY", "DATE"
        ])
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table)
        
        # Botones inferiores
        btn_refresh = QtWidgets.QPushButton("Actualizar Historial")
        btn_refresh.setStyleSheet(STYLE_BUTTON)
        btn_refresh.clicked.connect(self.load_history)
        layout.addWidget(btn_refresh)
        
        btn_export = QtWidgets.QPushButton("Exportar Historial a Excel")
        btn_export.setStyleSheet(STYLE_BUTTON)
        btn_export.clicked.connect(self.export_history_to_excel)
        layout.addWidget(btn_export)

    @staticmethod
    def custom_parse_date(s):
        """
        Intenta convertir la cadena s usando varios formatos posibles.
        Si ninguno funciona, devuelve pd.NaT.
        """
        s = s.strip()
        formats = [
            "%d/%m/%y %H:%M:%S",
            "%d/%m/%y %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%d/%m/%y"
        ]
        for fmt in formats:
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
        return pd.NaT

    def format_numeric(self, value):
        """
        Recibe un valor y, si es numérico, lo convierte a entero si la parte decimal es 0.
        Se retorna siempre como cadena para la visualización en la tabla.
        """
        try:
            num = float(value)
            if num.is_integer():
                return str(int(num))
            else:
                return str(num)
        except Exception:
            return str(value)

    def clean_numeric(self, value):
        """
        Convierte el valor a número. Si es numérico y no tiene parte decimal, lo retorna como entero.
        De lo contrario, devuelve el mismo valor (o con parte decimal preservada).
        """
        try:
            num = float(value)
            if num.is_integer():
                return int(num)
            else:
                return num
        except Exception:
            return value

    def apply_filters(self):
        try:
            df_hist = pd.read_csv(HISTORY_PATH, encoding="utf-8-sig", on_bad_lines='skip')
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo cargar el historial:\n{e}")
            return

        # Aseguramos que existan las columnas opcionales.
        for col in ["ESTADO", "COMENTARIO", "LINEA", "USER MFG"]:
            if col not in df_hist.columns:
                df_hist[col] = ""

        # Limpieza de la columna DATE: convertir a string y quitar espacios.
        df_hist["DATE"] = df_hist["DATE"].astype(str).str.strip()

        # Convertir DATE a datetime usando la función personalizada.
        df_hist["DATE_dt"] = df_hist["DATE"].apply(self.custom_parse_date)
        
        # Obtener los límites del filtro a partir de los QDateEdit.
        try:
            from_str = self.date_from.date().toString("dd/MM/yy")
            to_str = self.date_to.date().toString("dd/MM/yy")
            filter_from = datetime.strptime(from_str, "%d/%m/%y")
            filter_to = datetime.strptime(to_str, "%d/%m/%y") + timedelta(days=1)
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Error", f"Error al obtener las fechas del filtro:\n{e}")
            return

        df_hist = df_hist[(df_hist["DATE_dt"] >= filter_from) & (df_hist["DATE_dt"] < filter_to)]
        
        # Filtrar por MOVIMIENTO si el combobox no dice "Todos".
        mov = self.movimiento_combo.currentText().strip()
        if mov != "Todos":
            df_hist["MOVIMIENTO"] = df_hist["MOVIMIENTO"].astype(str).str.strip().str.lower()
            df_hist = df_hist[df_hist["MOVIMIENTO"] == mov.lower()]
        
        # Actualizar la columna JOB usando el mapping.
        job_list = []
        for idx, row in df_hist.iterrows():
            nomen = str(row.get("NOMENCLATURA", "")).strip()
            job = self.db_mapping.get(nomen, "")
            job_list.append(job)
        df_hist["JOB"] = job_list

        # Asegurarse de que NOMENCLATURA y JOB sean cadenas limpias.
        df_hist["NOMENCLATURA"] = df_hist["NOMENCLATURA"].astype(str).str.strip()
        df_hist["JOB"] = df_hist["JOB"].astype(str).str.strip()

        # Filtrar por término de búsqueda (si se ingresa) sobre NOMENCLATURA o JOB.
        search_term = self.search_edit.text().strip().lower()
        if search_term:
            mask = df_hist["NOMENCLATURA"].str.lower().str.contains(search_term, na=False) | \
                   df_hist["JOB"].str.lower().str.contains(search_term, na=False)
            df_hist = df_hist[mask]

        # Reordenar las columnas según el orden requerido.
        ordered_columns = [
            "History_ID", "USER", "NOMENCLATURA", "JOB", "LINEA", "USER MFG",
            "ESTADO", "MOVIMIENTO", "COMENTARIO", "QTY", "DATE"
        ]
        try:
            df_hist = df_hist[ordered_columns]
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Error", f"Error reordenando columnas:\n{e}")
            return

        self.populate_table(df_hist)

    def load_db_mapping(self):
        """Carga un diccionario que mapea NOMENCLATURA a JOB usando el CSV de herramental."""
        try:
            df_db = pd.read_csv(DB_PATH, encoding="utf-8-sig")
            if "NOMENCLATURA" in df_db.columns and "JOB" in df_db.columns:
                df_map = df_db[["NOMENCLATURA", "JOB"]].dropna().drop_duplicates()
                self.db_mapping = pd.Series(df_map.JOB.values, index=df_map.NOMENCLATURA).to_dict()
            else:
                self.db_mapping = {}
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "Error", f"No se pudo cargar la DB de herramental:\n{e}")
            self.db_mapping = {}

        # Actualizar el completer con la unión de nomenclaturas y jobs.
        union_list = list(set(list(self.db_mapping.keys()) + list(self.db_mapping.values())))
        model = QtCore.QStringListModel(union_list)
        self.search_completer.setModel(model)
    
    def load_history(self):
        try:
            df_hist = pd.read_csv(HISTORY_PATH, encoding="utf-8-sig", on_bad_lines='skip')
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"No se pudo cargar el historial:\n{e}")
            return
        # Asegurar que existan las columnas opcionales.
        for col in ["ESTADO", "COMENTARIO", "LINEA", "USER MFG"]:
            if col not in df_hist.columns:
                df_hist[col] = ""
        
        # Actualizar el combo de movimiento con movimientos únicos.
        movimientos = sorted(df_hist["MOVIMIENTO"].dropna().unique())
        self.movimiento_combo.clear()
        self.movimiento_combo.addItem("Todos")
        self.movimiento_combo.addItems(movimientos)
        
        # Insertar o actualizar la columna JOB usando el mapping.
        job_list = []
        for idx, row in df_hist.iterrows():
            nomen = str(row.get("NOMENCLATURA", "")).strip()
            job = self.db_mapping.get(nomen, "")
            job_list.append(job)
        if "JOB" not in df_hist.columns:
            df_hist.insert(3, "JOB", job_list)
        else:
            df_hist["JOB"] = job_list
        
        # Reordenar el DataFrame según el orden requerido.
        ordered_columns = [
            "History_ID", "USER", "NOMENCLATURA", "JOB", "LINEA", "USER MFG",
            "ESTADO", "MOVIMIENTO", "COMENTARIO", "QTY", "DATE"
        ]
        df_hist = df_hist[ordered_columns]
        
        self.populate_table(df_hist)
        
    def populate_table(self, df):
        self.table.setRowCount(0)
        self.table.setColumnCount(len(df.columns))
        self.table.setHorizontalHeaderLabels(df.columns.tolist())
        for i, (_, row) in enumerate(df.iterrows()):
            self.table.insertRow(i)
            for j, col in enumerate(df.columns):
                # Si la columna es "LINEA", "USER MFG" o "QTY", se aplica el formateo.
                if col in ["LINEA", "USER MFG", "QTY"]:
                    text = self.format_numeric(row[col])
                else:
                    text = str(row[col])
                item = QtWidgets.QTableWidgetItem(text)
                self.table.setItem(i, j, item)
        self.table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)
        # Opcional: ocultar la columna "History_ID" si no se requiere mostrarla.
        if "History_ID" in df.columns:
            col_index = list(df.columns).index("History_ID")
            self.table.setColumnHidden(col_index, True)
        self.current_export_df = df.copy()
        self.table.scrollToBottom()

    def export_history_to_excel(self):
        options = ["Exportar todo el historial", "Exportar historial filtrado"]
        option, ok = QtWidgets.QInputDialog.getItem(
            self, "Exportar Historial",
            "Seleccione una opción:", options, 0, False
        )
        if not ok or not option:
            return

        try:
            if option == "Exportar todo el historial":
                df_export = pd.read_csv(HISTORY_PATH, encoding="utf-8-sig", on_bad_lines='skip')
                for col in ["ESTADO", "COMENTARIO", "LINEA", "USER MFG"]:
                    if col not in df_export.columns:
                        df_export[col] = ""
                job_list = []
                for idx, row in df_export.iterrows():
                    nomen = str(row.get("NOMENCLATURA", "")).strip()
                    job = self.db_mapping.get(nomen, "")
                    job_list.append(job)
                if "JOB" not in df_export.columns:
                    df_export.insert(3, "JOB", job_list)
                else:
                    df_export["JOB"] = job_list
            else:
                if self.current_export_df.empty:
                    df_export = pd.read_csv(HISTORY_PATH, encoding="utf-8-sig", on_bad_lines='skip')
                    for col in ["ESTADO", "COMENTARIO", "LINEA", "USER MFG"]:
                        if col not in df_export.columns:
                            df_export[col] = ""
                    job_list = []
                    for idx, row in df_export.iterrows():
                        nomen = str(row.get("NOMENCLATURA", "")).strip()
                        job = self.db_mapping.get(nomen, "")
                        job_list.append(job)
                    if "JOB" not in df_export.columns:
                        df_export.insert(3, "JOB", job_list)
                    else:
                        df_export["JOB"] = job_list
                else:
                    df_export = self.current_export_df.copy()
            
            # Reordenar el DataFrame según el orden requerido.
            ordered_columns = [
                "History_ID", "USER", "NOMENCLATURA", "JOB", "LINEA", "USER MFG",
                "ESTADO", "MOVIMIENTO", "COMENTARIO", "QTY", "DATE"
            ]
            df_export = df_export[ordered_columns].copy()
            
            # Aplicar la conversión para las columnas numéricas (sin decimales si corresponde)
            for col in ["LINEA", "USER MFG", "QTY"]:
                if col in df_export.columns:
                    df_export[col] = df_export[col].apply(self.clean_numeric)
            
            # Exportamos sin forzar todo a string, conservando el tipo correcto.
            desktop = os.path.join(os.path.expanduser("~"), "Desktop")
            filename = os.path.join(desktop, "Historial_ToolTrack+.xlsx")
            df_export.to_excel(filename, index=False)
            QtWidgets.QMessageBox.information(self, "Exportación exitosa", 
                f"El historial se ha exportado en:\n{filename}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", 
                f"Error al exportar el historial:\n{e}")


class WindowItemWidget(QtWidgets.QWidget):
    def __init__(self, name, desc="", icon_path="", parent=None):
        """
        Crea un widget que muestra el nombre, la descripción y el ícono.
        Implementa:
         - Un tooltip que se muestra tras mantener el cursor 3 segundos.
         - Un efecto hover que resalta sutilmente el contorno y fondo del widget,
           sin alterar la posición de los componentes.
         - Iconos y textos de mayor tamaño.
         - No modifica el padding del icon_label al hacer hover.
         - El color del tooltip se adapta (si es posible) al fondo oscuro del efecto hover.
        """
        super().__init__(parent)
        # Permite que el widget pinte su propio fondo
        self.setAttribute(QtCore.Qt.WA_StyledBackground, True)
        # Asigna un nombre de objeto para aplicar reglas específicas en el style sheet
        self.setObjectName("WindowItemWidget")

        self.name = name
        self.desc = desc

        # Configurar tooltip
        self.setToolTip(f"{name}\n{desc}" if desc else name)
        try:
            QtWidgets.QToolTip.setStyleSheet(
                "QToolTip {"
                " background-color: rgba(46,46,46,77);"
                " color: white;"
                " border: 1px solid rgba(64,64,64,77);"
                " padding: 5px;"
                "}"
            )
        except AttributeError:
            pass

        # QTimer para mostrar el tooltip con retardo (3 segundos)
        self.tooltip_timer = QtCore.QTimer(self)
        self.tooltip_timer.setSingleShot(True)
        self.tooltip_timer.timeout.connect(self.show_delayed_tooltip)

        # Layout principal con márgenes en cero para evitar reflujo
        layout = QtWidgets.QHBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setSpacing(5)

        # Ícono (más grande y sin márgenes internos)
        self.icon_label = QtWidgets.QLabel()
        self.icon_label.setObjectName("iconLabel")
        self.icon_label.setFixedSize(50, 50)
        self.icon_label.setContentsMargins(0, 0, 0, 0)
        if not icon_path or not os.path.exists(icon_path):
            icon_path = r"C:\TOOLTRACK+\Recursos\LOGOS\ToolTrack+_logo.ico"  # Fallback
        pixmap = QtGui.QPixmap(icon_path)
        if not pixmap.isNull():
            self.icon_label.setPixmap(pixmap.scaled(
                self.icon_label.size(),
                QtCore.Qt.KeepAspectRatio,
                QtCore.Qt.SmoothTransformation
            ))
        layout.addWidget(self.icon_label)

        # Título (texto agrandado y sin márgenes ni bordes)
        self.title_label = QtWidgets.QLabel(name)
        self.title_label.setFixedHeight(30)
        self.title_label.setStyleSheet("font-size: 24px; margin: 0; border: none; background: transparent;")
        layout.addWidget(self.title_label)

        # Descripción (texto agrandado y sin márgenes ni bordes)
        self.desc_label = QtWidgets.QLabel(desc)
        self.desc_label.setFixedHeight(30)
        self.desc_label.setStyleSheet("font-size: 14px; margin: 0; border: none; background: transparent;")
        layout.addWidget(self.desc_label)

    def enterEvent(self, event):
        super().enterEvent(event)
        # Al entrar, restaura cualquier override (por ejemplo, de redimensionamiento)
        QtWidgets.QApplication.restoreOverrideCursor()
        # Establece el cursor de mano (PointingHand)
        self.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        # Aplica el styleSheet hover
        self.setStyleSheet(
            "#WindowItemWidget {"
            " border: 2px solid rgba(64, 64, 64, 77);"
            " background-color: rgba(46, 46, 46, 77);"
            "}"
            "#WindowItemWidget > QLabel#iconLabel {"
            " padding: 0px; margin: 0px;"
            "}"
        )
        self.tooltip_timer.start(3000)

    def leaveEvent(self, event):
        super().leaveEvent(event)
        # Restaura los estilos por defecto
        self.setStyleSheet("")
        if self.tooltip_timer.isActive():
            self.tooltip_timer.stop()
        QtWidgets.QToolTip.hideText()
        # Quita el cursor específico, de modo que permita que la lógica global (en main window) se haga cargo
        self.unsetCursor()
        QtWidgets.QApplication.restoreOverrideCursor()

    def show_delayed_tooltip(self):
        pos = QtGui.QCursor.pos()
        QtWidgets.QToolTip.showText(pos, self.toolTip(), self)

class OverviewPage(QtWidgets.QWidget):
    # Señal para indicar en qué módulo hacer clic.
    moduleNavigationRequested = pyqtSignal(int)

    def __init__(self, allowed_modules_definitions=[], parent=None):
        super().__init__(parent)
        self.allowed_modules_definitions = allowed_modules_definitions
        # Se elimina 'Inicio' de la lista a mostrar, ya que ya estamos en esa página.
        self.modules_to_display = [
            m for m in self.allowed_modules_definitions if m["name"] != "Inicio"
        ]
        self.setup_ui()

    def setup_ui(self):
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        # --- Frame Superior con fondo ---
        self.pinned_frame = QtWidgets.QFrame()
        self.pinned_frame.setStyleSheet(
            "background-image: url('C:/TOOLTRACK+/Recursos/LOGOS/Fondo app.jpg');"
            "background-repeat: no-repeat;"
            "background-position: center;"
            "border: 1px solid #ccc;"
        )
        self.pinned_frame.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        self.pinned_frame.setFixedHeight(80)
        pinned_layout = QtWidgets.QHBoxLayout(self.pinned_frame)
        pinned_layout.setContentsMargins(0, 0, 0, 0)
        pinned_layout.setSpacing(10)
        title_label = QtWidgets.QLabel("Módulos Disponibles")
        title_label.setStyleSheet("font-size: 24px; font-weight: bold; color: #ffffff;")
        title_label.setAlignment(QtCore.Qt.AlignCenter)
        pinned_layout.addWidget(title_label)
        layout.addWidget(self.pinned_frame)
        # -------------------------------------------------

        # --- Lista de Módulos ---
        all_frame = QtWidgets.QFrame()
        all_layout = QtWidgets.QVBoxLayout(all_frame)
        all_layout.setContentsMargins(5, 5, 5, 5)
        all_layout.setSpacing(10)

        self.list_widget = QtWidgets.QListWidget()
        # Evitar que se muestre la selección y el foco.
        self.list_widget.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)
        self.list_widget.setFocusPolicy(QtCore.Qt.NoFocus)
        self.list_widget.setStyleSheet(
            "QListWidget { border: none; background-color: transparent; }"
            "QListWidget::item:selected { background-color: transparent; }"
        )
        # Espaciado vertical entre botones
        self.list_widget.setSpacing(5)

        print(f"OverviewPage: Creando items para {len(self.modules_to_display)} módulos.")

        for module_def in self.modules_to_display:
            item = QtWidgets.QListWidgetItem(self.list_widget)
            # Aumenta la altura de cada item para separarlos verticalmente.
            item.setSizeHint(QtCore.QSize(200, 60))
            desc = module_def.get("desc", "")
            icon = module_def.get("icon", "")
            custom_widget = WindowItemWidget(module_def["name"], desc, icon)
            self.list_widget.setItemWidget(item, custom_widget)
            original_index = next(
                (i for i, m in enumerate(self.allowed_modules_definitions) if m["name"] == module_def["name"]),
                -1
            )
            if original_index != -1:
                item.setData(QtCore.Qt.UserRole, original_index)
            else:
                print(f"Advertencia: No se encontró el índice original para {module_def['name']}")
        
        self.list_widget.itemClicked.connect(self.overview_list_item_clicked)
        all_layout.addWidget(self.list_widget)
        layout.addWidget(all_frame)

    def overview_list_item_clicked(self, item):
        index = item.data(QtCore.Qt.UserRole)
        if index is not None and index >= 0:
            print(f"OverviewPage: Click en módulo con índice original {index}")
            self.moduleNavigationRequested.emit(index)
            # Quitar cualquier selección para evitar que se mantenga el resaltado azul.
            self.list_widget.clearSelection()
        else:
            print(f"OverviewPage: Click en item sin índice válido ({index}).")

class CustomScrollArea(QtWidgets.QScrollArea):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWidgetResizable(True)
        self.setFrameShape(QtWidgets.QFrame.NoFrame)  # Se quita el marco para que no afecte el ancho.

    def resizeEvent(self, event):
        super().resizeEvent(event)
        # Si el widget contenido cabe dentro del área visible, ocultamos la barra de desplazamiento.
        if self.widget() and self.widget().height() <= self.viewport().height():
            self.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        else:
            self.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)

# ================================================================
# Clase ToolTrackApp (Ventana principal - CON Lazy Loading y Pre-Carga Opcional de Inicio)
# ================================================================
class ToolTrackApp(QtWidgets.QMainWindow):

    # --- Métodos adjust_color y generate_button_style ---
    @staticmethod
    def adjust_color(hex_color, amount):
        # Asegurarse que el color es válido
        if not isinstance(hex_color, str) or not hex_color.startswith('#') or len(hex_color) != 7:
            hex_color = "#000000" # Fallback a negro si es inválido
        try:
            rgb = [int(hex_color[i:i+2], 16) for i in (1, 3, 5)]
            new_rgb = [min(255, max(0, c + amount)) for c in rgb]
            return f"#{new_rgb[0]:02x}{new_rgb[1]:02x}{new_rgb[2]:02x}"
        except ValueError:
            return hex_color # Devolver original si falla la conversión

    @staticmethod
    def generate_button_style(header_color):
        hover_color = ToolTrackApp.adjust_color(header_color, 20)
        pressed_color = ToolTrackApp.adjust_color(header_color, -20)
        style_text = f"""
        QPushButton {{
            background-color: {header_color}; color: white; border: none;
            border-radius: 1px; padding: 1px 1px; font: bold 10pt 'Montserrat';
        }}
        QPushButton:hover {{ background-color: {hover_color}; }}
        QPushButton:pressed {{ background-color: {pressed_color}; }}
        """
        return style_text
    # -------------------------------------------------------------

    def __init__(self, user_alias):
        super().__init__()
        self.user_alias = user_alias
        self.current_widget_instances = {} # Diccionario para widgets instanciados
        self.overview_page_instance = None # Para conectar la señal de OverviewPage

        # --- Verificar y Cargar Datos de Sesión ---
        if not (hasattr(Session, "user_data") and Session.user_data):
            # Es mejor manejar esto más elegantemente que un raise
            # Quizás mostrar mensaje y salir, o forzar re-login.
            print("Error crítico: No se encontró información de usuario en Session al iniciar ToolTrackApp.")
            QtWidgets.QMessageBox.critical(None, "Error Fatal", "No se pudo recuperar la información del usuario.")
            # En un caso real, podrías querer cerrar aquí:
            # QtCore.QTimer.singleShot(0, self.close) # Cerrar después de que el constructor termine
            # O si es seguro llamar directamente:
            # self.close() # Puede ser problemático dentro de __init__
            # Por ahora, continuamos pero la app podría fallar después.
            # Alternativa: sys.exit(1) si es aceptable cerrar todo.
            self.user_data = {} # Poner un diccionario vacío para evitar fallos posteriores
        else:
            self.user_data = Session.user_data
            print("Información de usuario precargada encontrada.")

        if not (hasattr(Session, "allowed_modules") and Session.allowed_modules):
            print("Error crítico: No se encontró lista de módulos permitidos en Session.")
            QtWidgets.QMessageBox.critical(None, "Error Fatal", "No se pudo recuperar la lista de módulos permitidos.")
            # Considerar cerrar aquí también
            self.allowed_modules_definitions = [] # Poner lista vacía
        else:
            self.allowed_modules_definitions = Session.allowed_modules
            print(f"ToolTrackApp: {len(self.allowed_modules_definitions)} definiciones de módulos cargadas.")
        # ---------------------------------------------

        # --- Configuración de la ventana ---
        self.current_header_color = "#d99227" # Color por defecto inicial
        self._drag_pos = None
        self._resize_margin = 8
        self._resize_direction = None
        self._resize_start = None
        self._start_geometry = None
        self.setWindowTitle(f"ToolTrack+ – Usuario: {self.user_alias}")
        self.setGeometry(100, 100, 1080, 720)
        self.setMinimumSize(400, 50)
        try:
            self.setWindowIcon(QtGui.QIcon(r'C:\TOOLTRACK+\Recursos\LOGOS\ToolTrack+_logo.ico'))
        except Exception as e:
            print(f"Advertencia: No se pudo cargar el ícono de la ventana: {e}")
        self.setWindowFlags(QtCore.Qt.FramelessWindowHint)
        self.setAttribute(QtCore.Qt.WA_TranslucentBackground)

        # Sombra
        shadow = QtWidgets.QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(15) # Un poco más de blur
        shadow.setColor(QtGui.QColor(0, 0, 0, 100)) # Sombra más sutil
        shadow.setXOffset(0)
        shadow.setYOffset(2) # Pequeño offset vertical
        self.central_widget = QtWidgets.QWidget()
        self.central_widget.setGraphicsEffect(shadow)
        self.setCentralWidget(self.central_widget)
        self.central_widget.setMouseTracking(True) # Para redimensionar desde el borde

        # Layout principal
        self.layout = QtWidgets.QVBoxLayout(self.central_widget)
        self.layout.setContentsMargins(0, 0, 0, 0) # Sin márgenes para que la sombra funcione bien
        self.layout.setSpacing(0)
        # -------------------------------------------------

        # Crear UI
        self.create_title_bar()
        self.create_main_area() # Crea sidebar y stack vacío
        self.load_initial_styles() # Carga y aplica estilos iniciales
        # self.setup_connections() # Se hacen dinámicamente en switch_page o aquí abajo

        # --- <<< INICIO: Cargar página inicial (con manejo de pre-carga) >>> ---
        initial_widget_loaded = False
        # Verificar si el diálogo de login pre-cargó la instancia de "Inicio"
        if hasattr(Session, 'initial_widget_instance') and Session.initial_widget_instance is not None:
            print("Usando instancia pre-cargada de 'Inicio'.")
            widget_instance = Session.initial_widget_instance
            # Obtener el índice original que guardamos en el login
            initial_index = getattr(Session, 'initial_widget_index', 0) # Usar 0 como fallback

            # Asegurarse que el índice es válido para el caché
            if not isinstance(initial_index, int) or initial_index < 0:
                 print(f"Advertencia: Índice pre-cargado inválido ({initial_index}), usando 0.")
                 initial_index = 0

            # Añadir al caché y al stack
            self.current_widget_instances[initial_index] = widget_instance
            self.content_stack.addWidget(widget_instance)
            widget_index_in_stack = self.content_stack.indexOf(widget_instance)

            if widget_index_in_stack != -1:
                 # Conectar señales específicas para OverviewPage aquí también
                 if isinstance(widget_instance, OverviewPage): # Asume que OverviewPage está importada/definida
                     try: # Asegurarse de no conectar dos veces si algo sale mal
                        widget_instance.moduleNavigationRequested.disconnect(self.switch_page)
                     except TypeError: pass # Ignorar si no estaba conectada
                     widget_instance.moduleNavigationRequested.connect(self.switch_page)
                     self.overview_page_instance = widget_instance # Guardar referencia
                     print("Conectada señal moduleNavigationRequested de OverviewPage (pre-cargada).")

                 print(f"Estableciendo widget actual a índice {widget_index_in_stack} en el stack (Inicio pre-cargado).")
                 self.content_stack.setCurrentIndex(widget_index_in_stack)
                 initial_widget_loaded = True
            else:
                # Esto sería un error inesperado si addWidget funcionó
                print("Error CRÍTICO: No se pudo encontrar la instancia pre-cargada en el stack después de añadirla.")
                # Limpiar Session para que el fallback funcione
                Session.initial_widget_instance = None
                Session.initial_widget_index = -1

        # Fallback: Si no se precargó o hubo un error al añadirla, usar el método normal
        if not initial_widget_loaded:
             print("Instancia 'Inicio' no pre-cargada o falló. Usando switch_page(0).")
             # Asegurarse que hay definiciones antes de llamar a switch_page
             if self.allowed_modules_definitions:
                 self.switch_page(0) # Asume que el índice 0 es 'Inicio'
             else:
                 print("Error: No hay definiciones de módulos para cargar la página inicial.")
                 # Podrías mostrar un widget de error aquí
        # --- <<< FIN: Cargar página inicial >>> ---
        self.center_on_screen()
        print("ToolTrackApp cargada y lista para mostrarse.")
        self.setMouseTracking(True)
        
    def center_on_screen(self):
        try:
            frame_geometry = self.frameGeometry()
            # Usar screenAt para obtener la pantalla donde está el cursor o la ventana
            # o QApplication.primaryScreen() si prefieres la principal siempre
            screen = QtWidgets.QApplication.screenAt(QtGui.QCursor.pos())
            if not screen:
                 screen = QtWidgets.QApplication.primaryScreen()
            center_point = screen.availableGeometry().center()
            frame_geometry.moveCenter(center_point)
            self.move(frame_geometry.topLeft())
        except Exception as e:
            print(f"Error al centrar la ventana: {e}. Usando geometría por defecto.")
            # self.setGeometry(100, 100, 1080, 720) # Ya se hizo antes

    def load_initial_styles(self):
        # --- Carga de estilos (Usa caché de usuarios) ---
        # Asumimos que load_user_data_by_email ya pobló el caché USER_DATA_CACHE
        global USER_DATA_CACHE # Asegurar acceso a la variable global
        header_color, sidebar_color, frame_color = '#d99227', '#f0f0f0', '#fcf6d7' # Defaults
        header_opacity, sidebar_opacity, frame_opacity = 100, 100, 100 # Defaults

        user_config = None
        if hasattr(Session, 'user_alias') and Session.user_alias and USER_DATA_CACHE:
            target_alias_upper = Session.user_alias.strip().upper()
            # Buscar por alias directamente si la estructura lo permite
            # O iterar si la clave es el email:
            for email, data in USER_DATA_CACHE.items():
                 alias_in_data = data.get("ALIAS") # Asume que ALIAS está en mayúsculas en el caché
                 if alias_in_data and str(alias_in_data).strip().upper() == target_alias_upper:
                     user_config = data
                     break # Encontrado

        if user_config:
            print(f"Cargando estilos guardados para {Session.user_alias}")
            # Obtener valores con fallback a los defaults definidos arriba
            header_color = user_config.get('CFG_HEADER_COLOR', header_color)
            sidebar_color = user_config.get('CFG_SIDEBAR_COLOR', sidebar_color)
            frame_color = user_config.get('CFG_FRAME_COLOR', frame_color)
            try: # Convertir opacidades a int, con fallback robusto
                header_opacity = int(user_config.get('CFG_HEADER_OPACITY', header_opacity))
                sidebar_opacity = int(user_config.get('CFG_SIDEBAR_OPACITY', sidebar_opacity))
                frame_opacity = int(user_config.get('CFG_FRAME_OPACITY', frame_opacity))
                # Validar rangos 0-100
                header_opacity = max(0, min(100, header_opacity))
                sidebar_opacity = max(0, min(100, sidebar_opacity))
                frame_opacity = max(0, min(100, frame_opacity))
            except (ValueError, TypeError):
                print("Advertencia: No se pudieron convertir los valores de opacidad a enteros, usando defaults.")
                header_opacity, sidebar_opacity, frame_opacity = 100, 100, 100
        else:
            print(f"No se encontró configuración de estilo para {Session.user_alias}, usando defaults.")

        # Llamar a update_styles AHORA que tenemos los colores/opacidades correctos
        self.update_styles(
            header_color, sidebar_color, frame_color,
            header_opacity, sidebar_opacity, frame_opacity
        )
        # Guardar el color actual del header para usarlo en generate_button_style si es necesario
        self.current_header_color = header_color
        # -------------------------------------------------------------

    def update_styles(self, header_color='#d99227', sidebar_color='#f0f0f0', frame_color='#fcf6d7', header_opacity=100, sidebar_opacity=100, frame_opacity=100):
        # --- Actualización de estilos ---
        print("update_styles llamado con:", header_color, sidebar_color, frame_color,
              header_opacity, sidebar_opacity, frame_opacity)
        try:
            # Validación simple de color (podría ser más robusta con regex)
            def validate_color(value, default):
                if isinstance(value, str) and value.startswith('#') and len(value) == 7:
                    try:
                        int(value[1:], 16) # Verificar si es hex válido
                        return value
                    except ValueError:
                        return default
                return default

            header_color = validate_color(header_color, "#d99227")
            sidebar_color = validate_color(sidebar_color, "#f0f0f0")
            frame_color  = validate_color(frame_color, "#fcf6d7")
            self.current_header_color = header_color # Actualizar el color actual

            # Convertir opacidad (0-100) a valor alpha (0.0-1.0) para estilos
            # RGBA: background-color: rgba(r, g, b, alpha);
            def hex_to_rgba(hex_c, opacity):
                if not isinstance(hex_c, str) or not hex_c.startswith('#') or len(hex_c) != 7:
                   hex_c = '#000000' # Fallback
                alpha = max(0.0, min(1.0, opacity / 100.0))
                try:
                    r = int(hex_c[1:3], 16)
                    g = int(hex_c[3:5], 16)
                    b = int(hex_c[5:7], 16)
                    return f"rgba({r}, {g}, {b}, {alpha})"
                except ValueError:
                    # Fallback si hex es inválido después de la validación inicial
                    alpha_str = f"{alpha:.2f}"
                    if header_color == '#d99227': return f"rgba(217, 146, 39, {alpha_str})"
                    if header_color == '#f0f0f0': return f"rgba(240, 240, 240, {alpha_str})"
                    if header_color == '#fcf6d7': return f"rgba(252, 246, 215, {alpha_str})"
                    return f"rgba(0, 0, 0, {alpha_str})" # Negro por defecto

            header_rgba = hex_to_rgba(header_color, header_opacity)
            sidebar_rgba = hex_to_rgba(sidebar_color, sidebar_opacity)
            frame_rgba = hex_to_rgba(frame_color, frame_opacity)

            # Aplicar colores/opacidad a los componentes principales
            if hasattr(self, 'title_bar'):
                # Usar rgba para transparencia si la opacidad no es 100
                title_style = f"background-color: {header_rgba}; border-bottom: 1px solid #ccc;"
                self.title_bar.setStyleSheet(title_style)

            if hasattr(self, 'sidebar'):
                sidebar_style = f"background-color: {sidebar_rgba}; border-right: 1px solid #ccc;"
                self.sidebar.setStyleSheet(sidebar_style)

            if hasattr(self, 'content_stack'): # El stack contiene las páginas
                 # El fondo del stack debe tener el color 'frame'
                 # ¡OJO! Si las páginas tienen su propio fondo opaco, este no se verá.
                 # Para transparencia general de la ventana, usar setWindowOpacity es mejor.
                 content_style = f"background-color: {frame_rgba};"
                 self.content_stack.setStyleSheet(content_style)

            # Opacidad general de la ventana (si frame_opacity < 100)
            # Esto afecta a TODA la ventana, incluyendo texto, bordes, etc.
            # Puede no ser lo deseado si solo quieres el fondo transparente.
            # self.setWindowOpacity(frame_opacity / 100.0) # Descomentar si se desea este efecto

            # Regenerar y aplicar estilo de botones de la barra de título
            # Los botones usan el color base del header (sin opacidad) para su fondo
            button_style = self.generate_button_style(header_color)
            if hasattr(self, 'minimize_button'): self.minimize_button.setStyleSheet(button_style)
            if hasattr(self, 'maximize_button'): self.maximize_button.setStyleSheet(button_style)
            if hasattr(self, 'close_button'): self.close_button.setStyleSheet(button_style)

            # Podrías necesitar actualizar estilos de otros widgets si dependen del tema

        except Exception as e:
            print(f"Error crítico actualizando estilos: {e}")
            traceback.print_exc()
        # -------------------------------------------------------------------

    def setup_connections(self):
        # Las conexiones principales (botones sidebar, señales de páginas)
        # se configuran en create_side_bar, __init__ (para pre-carga), y switch_page.
        # Este método podría usarse para conexiones globales si las hubiera.
        pass

    def eventFilter(self, obj, event):
        # Procesar eventos para botones
        if isinstance(obj, QtWidgets.QPushButton):
            if event.type() == QtCore.QEvent.Enter:
                # Al entrar en un botón, se restaura cualquier override global
                QtWidgets.QApplication.restoreOverrideCursor()
                obj.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
            elif event.type() == QtCore.QEvent.Leave:
                # Al salir del botón, se elimina su cursor específico
                obj.unsetCursor()

        # Para widgets críticos de main_area: sidebar y content_stack
        # Aquí agregamos MouseMove para delegar el evento al main window, de modo que se actualice el cursor.
        if event.type() == QtCore.QEvent.MouseMove:
            if hasattr(self, 'sidebar') and obj == self.sidebar:
                self.mouseMoveEvent(event)
            elif hasattr(self, 'content_stack') and obj == self.content_stack:
                self.mouseMoveEvent(event)
            # Si se tienen otros widgets con tracking (por ejemplo, custom items tipo WindowItemWidget)
            # se pueden agregar verificaciones similares:
            # elif isinstance(obj, WindowItemWidget):
            #     self.mouseMoveEvent(event)

        return super().eventFilter(obj, event)

    def create_main_area(self):
        # --- Área Principal (Crea sidebar y stack vacío) ---
        self.main_area = QtWidgets.QFrame()
        self.main_area.setObjectName("mainArea")  # Para posibles estilos
        self.main_area.setMouseTracking(True)
        self.main_area_layout = QtWidgets.QHBoxLayout(self.main_area)
        self.main_area_layout.setContentsMargins(0, 0, 0, 0)
        self.main_area_layout.setSpacing(0)

        # Sidebar se crea aquí
        self.sidebar = self.create_side_bar()
        self.sidebar.setMouseTracking(True)
        # Instalar event filter para que al mover el mouse sobre el sidebar se delegue el evento
        self.sidebar.installEventFilter(self)

        # Stack se crea vacío; los widgets se añadirán bajo demanda o por pre-carga
        self.content_stack = QtWidgets.QStackedWidget()
        self.content_stack.setObjectName("contentStack")  # Para posibles estilos
        self.content_stack.setMouseTracking(True)
        # Instalar event filter sobre content_stack
        self.content_stack.installEventFilter(self)

        self.main_area_layout.addWidget(self.sidebar)
        self.main_area_layout.addWidget(self.content_stack, 1)  # Darle más peso espacial al stack
        self.layout.addWidget(self.main_area)
        # -------------------------------------------------


    def create_title_bar(self):
        # --- Barra de título ---
        # Funciones auxiliares para sombra (pueden estar fuera si se reusan)
        def add_shadow_effect(widget):
            shadow = QtWidgets.QGraphicsDropShadowEffect(widget)
            shadow.setBlurRadius(8)
            shadow.setColor(QtGui.QColor(0, 0, 0, 80))
            shadow.setOffset(2, 2)
            widget.setGraphicsEffect(shadow)

        def _set_shadow_pressed(widget):
            effect = widget.graphicsEffect()
            if isinstance(effect, QtWidgets.QGraphicsDropShadowEffect):
                effect.setOffset(1, 1)
                effect.setColor(QtGui.QColor(0, 0, 0, 50))

        def _set_shadow_released(widget):
            effect = widget.graphicsEffect()
            if isinstance(effect, QtWidgets.QGraphicsDropShadowEffect):
                effect.setOffset(2, 2)
                effect.setColor(QtGui.QColor(0, 0, 0, 80))

        self.title_bar = QtWidgets.QFrame()
        self.title_bar.setObjectName("titleBar")  # Para estilos
        self.title_bar.setMouseTracking(True)
        # Estilo se aplica en update_styles o load_initial_styles
        self.title_bar.setFixedHeight(30)
        self.title_bar_layout = QtWidgets.QHBoxLayout(self.title_bar)
        self.title_bar_layout.setContentsMargins(5, 0, 5, 0)  # Margen derecho reducido
        self.title_bar_layout.setSpacing(5)

        # Etiqueta Usuario
        self.user_label = QtWidgets.QLabel(self.user_alias)
        self.user_label.setAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
        self.user_label.setStyleSheet("color: white; font: bold 10pt 'Montserrat'; padding-left: 10px;")
        self.title_bar_layout.addWidget(self.user_label)
        self.title_bar_layout.addStretch()

        # Etiqueta Título App
        self.title_label = QtWidgets.QLabel("ToolTrack+")
        self.title_label.setAlignment(QtCore.Qt.AlignCenter)
        self.title_label.setStyleSheet("color: white; font: bold 12pt 'Montserrat';")
        self.title_bar_layout.addWidget(self.title_label)
        self.title_bar_layout.addStretch()

        # Contenedor Botones (Min, Max, Close)
        self.buttons_container = QtWidgets.QWidget()
        self.buttons_layout = QtWidgets.QHBoxLayout(self.buttons_container)
        self.buttons_layout.setContentsMargins(0, 0, 0, 0)
        self.buttons_layout.setSpacing(5)

        # Estilo base de botones (se aplica en update_styles)
        button_style = self.generate_button_style(self.current_header_color)

        # Botón Minimizar
        self.minimize_button = QtWidgets.QPushButton("—")  # Guion largo
        self.minimize_button.setObjectName("titleBarButton")
        self.minimize_button.setFixedSize(20, 20)
        self.minimize_button.setStyleSheet(button_style)
        add_shadow_effect(self.minimize_button)
        self.minimize_button.pressed.connect(lambda: _set_shadow_pressed(self.minimize_button))
        self.minimize_button.released.connect(lambda: _set_shadow_released(self.minimize_button))
        self.minimize_button.clicked.connect(self.showMinimized)
        self.buttons_layout.addWidget(self.minimize_button)

        # Botón Maximizar/Restaurar
        self.maximize_button = QtWidgets.QPushButton("☐")  # Cuadrado
        self.maximize_button.setObjectName("titleBarButton")
        self.maximize_button.setFixedSize(20, 20)
        self.maximize_button.setStyleSheet(button_style)
        add_shadow_effect(self.maximize_button)
        self.maximize_button.pressed.connect(lambda: _set_shadow_pressed(self.maximize_button))
        self.maximize_button.released.connect(lambda: _set_shadow_released(self.maximize_button))
        self.maximize_button.clicked.connect(self.toggle_maximize)
        self.buttons_layout.addWidget(self.maximize_button)

        # Botón Cerrar
        self.close_button = QtWidgets.QPushButton("✕")  # Multiplicación
        self.close_button.setObjectName("titleBarButton")
        self.close_button.setFixedSize(20, 20)
        self.close_button.setStyleSheet(button_style)  # Aplicar estilo base
        add_shadow_effect(self.close_button)
        self.close_button.pressed.connect(lambda: _set_shadow_pressed(self.close_button))
        self.close_button.released.connect(lambda: _set_shadow_released(self.close_button))
        self.close_button.clicked.connect(self.close)
        self.buttons_layout.addWidget(self.close_button)

        self.title_bar_layout.addWidget(self.buttons_container)
        self.layout.addWidget(self.title_bar)

        # Delegación de eventos de ratón para mover/redimensionar desde title bar
        self.title_bar.mousePressEvent = self.title_bar_mousePressEvent
        self.title_bar.mouseMoveEvent = self.title_bar_mouseMoveEvent
        self.title_bar.mouseDoubleClickEvent = self.title_bar_mouseDoubleClickEvent
        self.title_bar.mouseReleaseEvent = self.title_bar_mouseReleaseEvent
# ----------------------------------------------------------------------

    def create_side_bar(self):
        # --- Barra Lateral (Usa definiciones de módulos con scroll condicional) ---
        # Creamos el widget contenedor de la barra lateral
        side_bar_content = QtWidgets.QFrame()
        side_bar_content.setObjectName("sideBarContent")
        side_bar_content.setFixedWidth(80)  # Ancho fijo para el contenido de la barra lateral

        # Layout para el contenido
        layout = QtWidgets.QVBoxLayout(side_bar_content)
        layout.setContentsMargins(5, 10, 5, 10)  # Márgenes verticales y laterales
        layout.setSpacing(8)  # Espaciado entre botones

        # Iterar sobre las definiciones de módulos permitidos
        if not self.allowed_modules_definitions:
            print("Advertencia: No hay definiciones de módulos para crear la barra lateral.")
            # Opcionalmente, añadir una etiqueta de alerta
        else:
            for idx, module_def in enumerate(self.allowed_modules_definitions):
                button = QtWidgets.QPushButton()
                button.setFixedSize(60, 60)  # Botones más grandes
                button.setToolTip(module_def.get("desc", module_def["name"]))
                button.setStyleSheet("""
                    QPushButton {
                        background-color: white; border: 1px solid #ccc;
                        border-radius: 8px;
                        padding: 5px;
                    }
                    QPushButton:hover { background-color: #e8e8e8; border: 1px solid #bbb; }
                    QPushButton:pressed { background-color: #d0d0d0; border: 1px solid #aaa; }
                    QPushButton:checked {
                        background-color: #c6c2f3;
                        border: 1px solid #a498fe;
                    }
                """)
                icon_path = module_def.get("icon", "")
                icon = QtGui.QIcon()
                if icon_path:
                    if os.path.exists(icon_path):
                        icon.addPixmap(QtGui.QPixmap(icon_path))
                    else:
                        print(f"Advertencia: Ícono no encontrado para {module_def['name']}: {icon_path}")
                if not icon.isNull():
                    button.setIcon(icon)
                    button.setIconSize(QtCore.QSize(48, 48))
                    button.setText("")
                else:
                    button.setText(module_def['name'][:3])
                    button.setIconSize(QtCore.QSize(0, 0))
                    button.setStyleSheet(button.styleSheet() + "QPushButton { font-size: 10pt; }")
                button.clicked.connect(lambda checked, index=idx: self.switch_page(index))
                # Opcional: instalar eventFilter para efectos de cursor en botones
                button.installEventFilter(self)
                layout.addWidget(button, alignment=QtCore.Qt.AlignCenter)
        layout.addStretch()  # Empuja los botones hacia arriba

        # Creamos el CustomScrollArea que contendrá side_bar_content
        scroll_area = CustomScrollArea()
        scroll_area.setWidget(side_bar_content)
        # Deshabilitar la barra horizontal (para preservar el ancho)
        scroll_area.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        # Fijamos un ancho igual al del contenido si se oculta la barra (el CustomScrollArea no añadirá ancho extra)
        scroll_area.setFixedWidth(80)
        return scroll_area
            # --------------------------------------------------

    # --- create_content_stack REMOVIDO (se crea vacío en create_main_area) ---

    def switch_page(self, index):
        # --- Cambiar Página (Implementa Lazy Loading y Timing) ---
        # Verificar si el índice es válido
        if not isinstance(index, int) or not (0 <= index < len(self.allowed_modules_definitions)):
             print(f"Error: Índice de página inválido o fuera de rango: {index}")
             return

        module_definition = self.allowed_modules_definitions[index]
        module_name = module_definition.get("name", f"Módulo Desconocido {index}") # Usar .get con fallback
        print(f"Intentando cambiar a la página: {module_name} (índice {index})")

        # Verificar si el widget ya fue instanciado y está en el caché
        widget_instance = self.current_widget_instances.get(index)

        if widget_instance is None:
            # El widget no existe, hay que crearlo e insertarlo
            print(f"Instanciando widget para: {module_name}")
            factory = module_definition.get("widget_factory")
            args = module_definition.get("widget_args", []) # Obtener args definidos

            # Si la fábrica es OverviewPage, asegurarse de pasarle las definiciones correctas
            # Esto ahora se maneja al definir los args en _load_allowed_modules del Login
            # if factory is OverviewPage: # Comparar con la clase directamente
            #    args = [self.allowed_modules_definitions] # Pasar definiciones como argumento

            if factory:
                try:
                    # --- INICIO Timing ---
                    start_time = time.perf_counter()
                    # ---

                    # Crear la instancia del widget usando la fábrica y los argumentos
                    widget_instance = factory(*args) # Desempaquetar argumentos

                    # --- FIN Timing ---
                    end_time = time.perf_counter()
                    duration = end_time - start_time
                    print(f"TIMING: Widget '{module_name}' instanciado en {duration:.4f} segundos.")
                    # ---

                    # Guardar la instancia en el caché
                    self.current_widget_instances[index] = widget_instance
                    # Añadir el nuevo widget al QStackedWidget
                    self.content_stack.addWidget(widget_instance)
                    print(f"Widget '{module_name}' añadido al stack.")

                    # --- Conexiones específicas post-instanciación ---
                    # Conectar señal de OverviewPage si es el módulo de Inicio
                    if module_name == "Inicio" and isinstance(widget_instance, OverviewPage):
                         try: widget_instance.moduleNavigationRequested.disconnect(self.switch_page)
                         except TypeError: pass
                         widget_instance.moduleNavigationRequested.connect(self.switch_page)
                         self.overview_page_instance = widget_instance
                         print("Conectada señal moduleNavigationRequested de OverviewPage.")

                    # Conectar señal de Window8Page (Configuración) si existe
                    if module_name == "Configuración y Personalización" and hasattr(widget_instance, 'config_updated'):
                         try: widget_instance.config_updated.disconnect(self.update_styles)
                         except TypeError: pass
                         widget_instance.config_updated.connect(self.update_styles)
                         print("Conectada señal config_updated de Window8Page.")
                    # -------------------------------------------------

                except Exception as e:
                    print(f"Error CRÍTICO al instanciar el widget para {module_name}: {e}")
                    traceback.print_exc() # Imprimir stack trace completo
                    QtWidgets.QMessageBox.critical(self, "Error de Módulo", f"No se pudo cargar el módulo '{module_name}'.\n\nError: {e}\n\nConsulte la consola para más detalles.")
                    # No añadir widget_instance si falló la creación
                    widget_instance = None # Asegurarse que es None para la lógica posterior
                    # Considerar si remover la entrada del caché si se añadió erróneamente
                    # if index in self.current_widget_instances:
                    #    del self.current_widget_instances[index]
                    # No retornar aquí todavía, dejar que la lógica final maneje el None
            else:
                print(f"Error: No se definió 'widget_factory' para el módulo {module_name}")
                QtWidgets.QMessageBox.warning(self, "Error de Configuración", f"El módulo '{module_name}' no está configurado correctamente (falta widget_factory).")
                # Podría mostrar un widget placeholder
                # widget_instance = PlaceholderWidget(f"Error: Módulo '{module_name}' sin fábrica.")
                # self.current_widget_instances[index] = widget_instance
                # self.content_stack.addWidget(widget_instance)
                widget_instance = None # Indicar que no hay instancia válida

        # --- Fin de la creación/obtención del widget ---

        if widget_instance: # Solo proceder si tenemos una instancia válida (creada ahora o del caché)
            # Establecer el widget actual en el QStackedWidget
            # Necesitamos obtener el índice DENTRO del stack, no el índice original del módulo
            widget_index_in_stack = self.content_stack.indexOf(widget_instance)
            if widget_index_in_stack != -1:
                print(f"Estableciendo widget actual a índice {widget_index_in_stack} en el stack ({module_name}).")
                self.content_stack.setCurrentIndex(widget_index_in_stack)
                # Opcional: Marcar botón activo en la sidebar
                # self.update_sidebar_selection(index) # Necesitarías implementar esto
            else:
                # Esto no debería ocurrir si la lógica anterior funcionó
                print(f"Error CRÍTICO: No se pudo encontrar el widget '{module_name}' en el stack después de añadirlo/obtenerlo.")
                QtWidgets.QMessageBox.critical(self, "Error Interno", f"No se pudo mostrar el módulo '{module_name}' porque no se encontró en el stack.")
        else:
             print(f"Advertencia: No se cambió de página porque no se pudo obtener o crear una instancia válida para el módulo {module_name} (índice {index}).")
             # No hacemos nada si no hay widget válido que mostrar

    # --------------------------------------------------
    def mousePressEvent(self, event):
        if event.button() == QtCore.Qt.LeftButton:
            # Convertir la posición del evento a coordenadas relativas de la ventana principal,
            # sin importar en qué widget interno se haya hecho clic.
            pos = self.mapFromGlobal(event.globalPos())
            margin = self._resize_margin
            rect = self.rect()
            direction = ""

            # Detección de borde con respecto al rectángulo externo de la ventana
            on_left   = pos.x() <= margin
            on_right  = pos.x() >= rect.width() - margin
            on_top    = pos.y() <= margin
            on_bottom = pos.y() >= rect.height() - margin

            if on_top:    direction += "T"
            if on_bottom: direction += "B"
            if on_left:   direction += "L"
            if on_right:  direction += "R"

            if direction:
                # Se ignora la detección de resize si el clic se hizo dentro de ciertos frames internos

                # Convertir la geometría de la barra de título al sistema de coordenadas de la ventana
                if hasattr(self, 'title_bar'):
                    title_bar_geo = QtCore.QRect(self.title_bar.mapTo(self, QtCore.QPoint(0, 0)), self.title_bar.size())
                    # Si se hizo clic en la title bar y no es el borde superior (T), ignoramos el resize
                    if title_bar_geo.contains(pos) and direction != "T":
                        super().mousePressEvent(event)
                        return

                # Convertir la geometría del contenedor de botones para evitar errores en la zona de éstos
                if hasattr(self, 'buttons_container'):
                    buttons_geo = QtCore.QRect(self.buttons_container.mapTo(self, QtCore.QPoint(0, 0)), self.buttons_container.size())
                    if buttons_geo.contains(pos):
                        super().mousePressEvent(event)
                        return

                # Inicia el proceso de resize
                self._resize_direction = direction
                self._resize_start = event.globalPos()
                self._start_geometry = self.geometry()
                event.accept()
                return

        super().mousePressEvent(event)


    def mouseMoveEvent(self, event):
        margin = self._resize_margin
        # Convertir la posición global al sistema de coordenadas de la ventana principal.
        pos = self.mapFromGlobal(event.globalPos())
        
        if self._resize_direction:
            # Calcular la diferencia en posición utilizando las coordenadas globales.
            diff = event.globalPos() - self._resize_start
            diff_x = diff.x()
            diff_y = diff.y()
            x, y, w, h = self._start_geometry.getRect()
            
            # Valores mínimos para ancho y alto.
            min_width = max(200, self.minimumSizeHint().width())
            min_height = max(50, self.minimumSizeHint().height())
            
            # Inicializar nuevos valores con los actuales.
            new_x = x
            new_y = y
            new_w = w
            new_h = h
            
            # Si se está redimensionando desde la parte izquierda, mover la izquierda y disminuir el ancho.
            if "L" in self._resize_direction:
                new_x = x + diff_x
                new_w = max(min_width, w - diff_x)
            # Si se redimensiona desde la parte derecha, simplemente aumentar o disminuir el ancho.
            if "R" in self._resize_direction:
                new_w = max(min_width, w + diff_x)
            # Para la parte superior, mover la posición y aumentar el alto (si el mouse va hacia afuera).
            if "T" in self._resize_direction:
                new_y = y + diff_y
                new_h = max(min_height, h - diff_y)
            # Para la parte inferior, aumentar o disminuir la altura.
            if "B" in self._resize_direction:
                new_h = max(min_height, h + diff_y)
            
            self.setGeometry(new_x, new_y, new_w, new_h)
            event.accept()
            return
        else:
            # Evaluar la posición relativa respecto al main window para determinar si estamos en un borde.
            rect = self.rect()
            on_left   = pos.x() <= margin
            on_right  = pos.x() >= rect.width() - margin
            on_top    = pos.y() <= margin
            on_bottom = pos.y() >= rect.height() - margin
                       
            # Si no estamos en ninguna zona de borde, se restaura el cursor normal.
            if not (on_left or on_right or on_top or on_bottom):
                QtWidgets.QApplication.restoreOverrideCursor()
                event.accept()
                return
            
            # Seleccionar el cursor adecuado en función de la combinación de bordes:
            if (on_top and on_left) or (on_bottom and on_right):
                cursor = QtCore.Qt.SizeFDiagCursor
            elif (on_top and on_right) or (on_bottom and on_left):
                cursor = QtCore.Qt.SizeBDiagCursor
            elif on_left or on_right:
                cursor = QtCore.Qt.SizeHorCursor
            elif on_top or on_bottom:
                cursor = QtCore.Qt.SizeVerCursor
            else:
                cursor = QtCore.Qt.ArrowCursor
                QtWidgets.QApplication.restoreOverrideCursor()
                event.accept()
                return

            QtWidgets.QApplication.setOverrideCursor(QtGui.QCursor(cursor))
            event.accept()

    def mouseReleaseEvent(self, event):
        if event.button() == QtCore.Qt.LeftButton:
            if self._resize_direction:
                self._resize_direction = None
                self._resize_start = None
                self._start_geometry = None
                QtWidgets.QApplication.restoreOverrideCursor()
                event.accept()
                return
        QtWidgets.QApplication.restoreOverrideCursor()
        super().mouseReleaseEvent(event)

    def mainArea_mouseMoveEvent(self, event):
        """
        Este método se asigna a la propiedad mouseMoveEvent de main_area para que
        toda la información de movimiento se delegue al mouseMoveEvent del main window.
        De esta forma, la lógica de cambio de cursor (por ejemplo, cercanía a los
        bordes inferior, derecho o inferior derecho) se evaluará correctamente.
        """
        # Delegamos directamente el evento al método principal
        self.mouseMoveEvent(event)
        event.accept()

    def title_bar_mousePressEvent(self, event):
        if event.button() == QtCore.Qt.LeftButton:
            margin = self._resize_margin
            # Convertir la posición de la title bar a coordenadas globales y luego a las de la ventana principal
            global_pos = self.title_bar.mapToGlobal(event.pos())
            main_pos = self.mapFromGlobal(global_pos)

            # Si el clic se hace en el borde superior de la title bar, delega a la función principal
            if main_pos.y() <= margin and self.title_bar.y() == 0:
                new_event = QtGui.QMouseEvent(
                    QtCore.QEvent.MouseButtonPress,
                    main_pos,
                    global_pos,
                    QtCore.Qt.LeftButton,
                    QtCore.Qt.LeftButton,
                    event.modifiers()
                )
                self.mousePressEvent(new_event)
                if new_event.isAccepted():
                    event.accept()
                return
            else:
                # Si el clic se realiza sobre los botones, se ignora para permitir sus acciones
                if hasattr(self, 'buttons_container'):
                    buttons_geo = QtCore.QRect(self.buttons_container.mapTo(self, QtCore.QPoint(0, 0)),
                                                self.buttons_container.size())
                    if buttons_geo.contains(main_pos):
                        event.ignore()
                        return
                # Inicia movimiento (drag) de la ventana
                self._drag_pos = event.globalPos() - self.frameGeometry().topLeft()
                event.accept()

    def title_bar_mouseMoveEvent(self, event):
        if event.buttons() == QtCore.Qt.LeftButton:
            if self._resize_direction:
                new_event = QtGui.QMouseEvent(
                    QtCore.QEvent.MouseMove,
                    self.mapFromGlobal(event.globalPos()),
                    event.globalPos(),
                    QtCore.Qt.LeftButton,
                    QtCore.Qt.LeftButton,
                    event.modifiers()
                )
                self.mouseMoveEvent(new_event)
                event.accept()
                return
            elif self._drag_pos:
                self.move(event.globalPos() - self._drag_pos)
                event.accept()
                return
        # En cualquier otro caso, delegamos totalmente el movimiento al main window
        new_event = QtGui.QMouseEvent(
            event.type(),
            self.mapFromGlobal(event.globalPos()),
            event.globalPos(),
            event.button(),
            event.buttons(),
            event.modifiers()
        )
        self.mouseMoveEvent(new_event)
        event.accept()

    def title_bar_mouseReleaseEvent(self, event):
        if event.button() == QtCore.Qt.LeftButton:
            if self._resize_direction:
                new_event = QtGui.QMouseEvent(
                    QtCore.QEvent.MouseButtonRelease,
                    self.mapFromGlobal(event.globalPos()),
                    event.globalPos(),
                    QtCore.Qt.LeftButton,
                    QtCore.Qt.NoButton,
                    event.modifiers()
                )
                self.mouseReleaseEvent(new_event)
                if new_event.isAccepted():
                    event.accept()
                return
            elif self._drag_pos:
                self._drag_pos = None
                event.accept()
                return

    def title_bar_mouseDoubleClickEvent(self, event):
        # Ignorar si el doble clic se produce en el área de botones
        if hasattr(self, 'buttons_container') and self.buttons_container.geometry().contains(event.pos()):
            event.ignore()
            return
        if event.button() == QtCore.Qt.LeftButton:
            self.toggle_maximize()
            event.accept()

    def toggle_maximize(self):
        # ... (tu código original) ...
        if self.isMaximized() or self.isFullScreen():
            self.showNormal()
            self.maximize_button.setText("☐")
            self.maximize_button.setToolTip("Maximizar")
        else:
            # self._normal_geometry = self.geometry() # Opcional guardar geometría
            self.showMaximized()
            self.maximize_button.setText("❐") # Caracter para restaurar
            self.maximize_button.setToolTip("Restaurar")

    def resizeEvent(self, event):
        # ... (tu código original para bordes redondeados) ...
        # Crear máscara para bordes redondeados
        # Es importante que esto sea eficiente
        try:
            rect = self.rect()
            path = QtGui.QPainterPath()
            # Usar un radio razonable, 10-15 suele estar bien
            radius = 10
            # Añadir rectángulo redondeado (usar QRectF para precisión flotante)
            path.addRoundedRect(QtCore.QRectF(rect), radius, radius)
            # Crear región desde el path
            # Usar toFillPolygon puede ser costoso, probar con QRegion(path) si la versión de Qt lo soporta bien
            polygon = path.toFillPolygon(QtGui.QTransform()).toPolygon()
            if polygon: # Asegurarse que el polígono no está vacío
                region = QtGui.QRegion(polygon)
                self.setMask(region)
            else:
                 # Si falla la creación del polígono, quitar la máscara
                 self.clearMask()
        except Exception as e:
            print(f"Error en resizeEvent (setMask): {e}")
            self.clearMask() # Quitar máscara si hay error
        # Llamar al método base es importante para que el layout se ajuste
        super().resizeEvent(event)


    def keyPressEvent(self, event):
        # ... (tu código original para F11) ...
        if event.key() == QtCore.Qt.Key_F11:
            if self.isFullScreen():
                self.showNormal() # O showMaximized() si prefieres
                # Restaurar estado del botón maximizar si es necesario
                if self.isMaximized():
                     self.maximize_button.setText("❐")
                     self.maximize_button.setToolTip("Restaurar")
                else:
                     self.maximize_button.setText("☐")
                     self.maximize_button.setToolTip("Maximizar")
            else:
                self.showFullScreen()
                self.maximize_button.setText("❐") # Icono de restaurar en pantalla completa
                self.maximize_button.setToolTip("Salir Pantalla Completa")
            event.accept()
        else:
            super().keyPressEvent(event) # Pasar otras teclas
    # -------------------------------------------------------------------

# ================================================================
# Main (MODIFICADO para usar UserLoginDialog y manejar Session)
# ================================================================
if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    # Intenta establecer el estilo Fusion, si falla, usa el default
    try:
        app.setStyle("Fusion")
    except Exception as e:
        print(f"Advertencia: No se pudo establecer el estilo 'Fusion'. {e}")

    # --- Inicializar estado global/Session (ejemplo) ---
    # Asegúrate que la clase Session o el módulo existen y están inicializados
    # class Session: # Ejemplo simple
    #     user_data = None
    #     allowed_modules = None
    #     user_alias = None
    #     initial_widget_instance = None
    #     initial_widget_index = -1
    # global USER_DATA_CACHE # Asegúrate que está inicializado
    # USER_DATA_CACHE = {} # Ejemplo

    # --- Lanzar el diálogo de login ---
    main_login = UserLoginDialog() # Usar el diálogo renombrado

    if main_login.exec_() == QtWidgets.QDialog.Accepted:
        # El alias y los módulos permitidos (y opcionalmente initial_widget_instance)
        # ya deberían estar en la clase Session gracias a la lógica dentro de on_login_result

        # Doble verificación de datos esenciales en Session
        if not hasattr(Session, 'user_alias') or not Session.user_alias or \
           not hasattr(Session, 'allowed_modules') or Session.allowed_modules is None:
            # Considerar si allowed_modules puede ser una lista vacía válida
            QtWidgets.QMessageBox.critical(None, "Error Fatal",
                                           "No se pudo inicializar la sesión del usuario correctamente después del login.")
            sys.exit(1)

        print(f"Login exitoso para: {Session.user_alias}. Iniciando ventana principal...")
        # Pasar el alias a ToolTrackApp (aunque ya está en Session, puede ser útil tenerlo directo)
        window = ToolTrackApp(Session.user_alias)
        window.show()
        sys.exit(app.exec_())
    else:
        # El usuario cerró el diálogo o hubo un error irrecuperable
        print("Login cancelado o fallido. Saliendo.")
        sys.exit(0)
