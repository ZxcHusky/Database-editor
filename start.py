import sys
import os
import pandas as pd
import sqlite3
from functools import partial
import re
import requests
import json

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QComboBox, QPushButton, QListWidget, QLineEdit, QProgressBar,
    QFileDialog, QMessageBox, QDialog, QCheckBox, QTableWidget, QTableWidgetItem,
    QHeaderView, QGroupBox, QInputDialog, QStyleFactory, QAbstractItemView,
    QTabWidget, QTextEdit, QSplitter, QTextBrowser
)
from PySide6.QtCore import Qt, QSize, QThread, Signal
from PySide6.QtGui import QColor, QLinearGradient, QBrush, QPainter, QFont, QPalette, QIcon, QTextCursor

t = {
    'en': {
        'title': 'Data Converter Pro',
        'file_selection': 'File Selection',
        'add_files': 'Add File(s)',
        'clear_list': 'Clear List',
        'options': 'Conversion Options',
        'txt_delimiter': 'TXT Delimiter:',
        'custom_header': 'Custom Header (comma-separated):',
        'row_filter': 'Row Filter (Pandas query):',
        'preview': 'Preview Selected',
        'convert': 'Convert All to CSV',
        'convert_merge': 'Merge to Single CSV (Low Memory)',
        'language': 'Language:',
        'unsupported': 'Unsupported file type:',
        'no_selection': 'Please select a file to preview.',
        'no_files': 'No files to convert.',
        'done': 'All files have been converted!',
        'merge_done': 'Files merged successfully with low memory usage!',
        'preview_title': 'Preview: {}',
        'json_orient': 'Enter pandas JSON orient (records,index,columns):',
        'select_sheet': 'Available sheets: {sheets}\nEnter sheet name:',
        'select_table': 'Available tables: {tables}\nEnter table name:',
        'merge_db': 'Merge Databases',
        'select_output': 'Select output file name',
        'merge_title': 'Merge SQL Databases',
        'merge_help': 'Select databases to merge (first will be base)',
        'select_tables': 'Select tables to merge:',
        'output_name': 'Output name:',
        'merge': 'Merge',
        'chunk_size': 'Chunk size (rows):',
        'error': 'Error',
        'info': 'Information',
        'db_operations': 'Database Operations',
        'open_db': 'Open Database',
        'find_duplicates': 'Find Duplicates',
        'db_info': 'DB: {tables} tables, {rows} rows in {table}',
        'db_info_no_table': 'DB: {tables} tables',
        'select_table_for_open': 'Select table to open:',
        'select_table_for_duplicates': 'Select table to find duplicates:',
        'duplicates_found': 'Duplicates found: {count}',
        'no_duplicates': 'No duplicates found',
        'db_info_short': 'DB: {tables} tables',
        'table_info': 'Table: {table}, Rows: {rows}, Columns: {cols}',
        'ai_chat': 'AI Chat',
        'send': 'Send',
        'clear_chat': 'Clear Chat',
        'enter_message': 'Enter your message...',
        'api_error': 'API Error: {error}',
        'thinking': 'Thinking...',
        'tabs': {
            'converter': 'Data Converter',
            'ai_chat': 'AI Assistant'
        },
        'models': {
            'gpt-4o-mini': 'GPT-4o Mini',
            'gpt-3.5-turbo': 'GPT-3.5 Turbo',
            'claude-3-haiku': 'Claude 3 Haiku'
        },
        'ai_system_prompt': 'You are a helpful assistant specialized in data processing and file conversion. Provide concise and accurate answers.'
    },
    'ru': {
        'title': 'Конвертер данных Pro',
        'file_selection': 'Выбор файлов',
        'add_files': 'Добавить файл(ы)',
        'clear_list': 'Очистить список',
        'options': 'Параметры конвертации',
        'txt_delimiter': 'Разделитель TXT:',
        'custom_header': 'Свои заголовки (через запятую):',
        'row_filter': 'Фильтр строк (Pandas query):',
        'preview': 'Предпросмотр',
        'convert': 'Конвертировать все',
        'convert_merge': 'Объединить в один CSV (экономно)',
        'language': 'Язык:',
        'unsupported': 'Неподдерживаемый тип файла:',
        'no_selection': 'Выберите файл для предпросмотра.',
        'no_files': 'Нет файлов для конвертации.',
        'done': 'Конвертация завершена!',
        'merge_done': 'Файлы объединены с минимальным использованием памяти!',
        'preview_title': 'Предпросмотр: {}',
        'json_orient': 'Введите ориентацию JSON (records,index,columns):',
        'select_sheet': 'Доступные листы: {sheets}\nВведите имя листа:',
        'select_table': 'Доступные таблицы: {tables}\nВведите имя таблицы:',
        'merge_db': 'Объединить базы',
        'select_output': 'Выберите имя файла',
        'merge_title': 'Объединение баз SQL',
        'merge_help': 'Выберите базы для объединения (первая будет основной)',
        'select_tables': 'Выберите таблицы для объединения:',
        'output_name': 'Имя файла:',
        'merge': 'Объединить',
        'chunk_size': 'Размер блока (строк):',
        'error': 'Ошибка',
        'info': 'Информация',
        'db_operations': 'Операции с базой данных',
        'open_db': 'Открыть базу',
        'find_duplicates': 'Найти дубликаты',
        'db_info': 'БД: {tables} таблиц, {rows} строк в {table}',
        'db_info_no_table': 'БД: {tables} таблиц',
        'select_table_for_open': 'Выберите таблицу для открытия:',
        'select_table_for_duplicates': 'Выберите таблицу для поиска дубликатов:',
        'duplicates_found': 'Найдено дубликатов: {count}',
        'no_duplicates': 'Дубликаты не найдены',
        'db_info_short': 'БД: {tables} таблиц',
        'table_info': 'Таблица: {table}, Строк: {rows}, Столбцов: {cols}',
        'ai_chat': 'Чат с ИИ',
        'send': 'Отправить',
        'clear_chat': 'Очистить чат',
        'enter_message': 'Введите ваше сообщение...',
        'api_error': 'Ошибка API: {error}',
        'thinking': 'Думаю...',
        'tabs': {
            'converter': 'Конвертер данных',
            'ai_chat': 'ИИ Ассистент'
        },
        'models': {
            'gpt-4o-mini': 'GPT-4o Mini',
            'gpt-3.5-turbo': 'GPT-3.5 Turbo',
            'claude-3-haiku': 'Claude 3 Haiku'
        },
        'ai_system_prompt': 'Вы полезный помощник, специализирующийся на обработке данных и преобразовании файлов. Давайте краткие и точные ответы.'
    }
}

class GradientWidget(QWidget):
    def paintEvent(self, event):
        painter = QPainter(self)
        gradient = QLinearGradient(0, 0, self.width(), self.height())
        gradient.setColorAt(0.0, QColor(30, 40, 60))
        gradient.setColorAt(0.5, QColor(60, 80, 120))
        gradient.setColorAt(1.0, QColor(40, 50, 80))
        painter.fillRect(self.rect(), QBrush(gradient))

class PreviewDialog(QDialog):
    def __init__(self, title, df, parent=None, full_table=False):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setMinimumSize(800, 600)
        
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        if full_table:
            info_label = QLabel(f"Rows: {len(df)}, Columns: {len(df.columns)}")
            info_label.setStyleSheet("color: white; font-weight: bold; padding: 5px;")
            layout.addWidget(info_label)
        
        self.table = QTableWidget()
        self.table.setRowCount(len(df) if full_table else min(100, len(df)))
        self.table.setColumnCount(len(df.columns))
        self.table.setHorizontalHeaderLabels(df.columns)
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Stretch)
        header.setStyleSheet("""
            QHeaderView::section {
                background-color: #2a3b5c;
                color: white;
                padding: 4px;
                border: 1px solid #3a4b6c;
                font-weight: bold;
            }
        """)
        
        row_count = len(df) if full_table else min(100, len(df))
        for i in range(row_count):
            for j, col in enumerate(df.columns):
                item = QTableWidgetItem(str(df.iloc[i, j]))
                item.setForeground(QColor(220, 220, 220))
                self.table.setItem(i, j, item)
        
        self.table.setStyleSheet("""
            QTableWidget {
                background-color: #1e2838;
                color: #e0e0e0;
                gridline-color: #3a4b6c;
                font-size: 11px;
            }
            QTableWidget::item {
                border-bottom: 1px solid #3a4b6c;
            }
        """)
        
        layout.addWidget(self.table)

class MergeDialog(QDialog):
    def __init__(self, title, tables, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setMinimumSize(400, 500)
        
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        label = QLabel(parent.txt['select_tables'])
        label.setStyleSheet("color: white; font-size: 12px;")
        layout.addWidget(label)
        
        self.checkboxes = {}
        for table in tables:
            cb = QCheckBox(table)
            cb.setChecked(True)
            cb.setStyleSheet("color: white;")
            layout.addWidget(cb)
            self.checkboxes[table] = cb
        
        self.merge_btn = QPushButton(parent.txt['merge'])
        self.merge_btn.setStyleSheet(parent.button_style)
        self.merge_btn.clicked.connect(self.accept)
        layout.addWidget(self.merge_btn)
    
    def get_selected_tables(self):
        return [table for table, cb in self.checkboxes.items() if cb.isChecked()]

class DatabaseWorker(QThread):
    progress = Signal(int)
    result = Signal(dict)
    error = Signal(str)

    def __init__(self, path, operation):
        super().__init__()
        self.path = path
        self.operation = operation

    def run(self):
        try:
            conn = sqlite3.connect(self.path)
            cursor = conn.cursor()
            
            if self.operation == 'info':
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                result = {'operation': 'info', 'tables': [t[0] for t in tables]}
                
                table_info = {}
                for table in result['tables']:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    row_count = cursor.fetchone()[0]
                    cursor.execute(f"PRAGMA table_info({table})")
                    columns = cursor.fetchall()
                    table_info[table] = {
                        'rows': row_count,
                        'cols': len(columns)
                    }
                result['table_info'] = table_info
                self.result.emit(result)
                
            elif self.operation == 'duplicates':
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = [t[0] for t in cursor.fetchall()]
                
                duplicates = {}
                for i, table in enumerate(tables):
                    self.progress.emit(int((i / len(tables)) * 50))
                    
                    cursor.execute(f"PRAGMA table_info({table})")
                    columns = [col[1] for col in cursor.fetchall()]
                    
                    group_columns = ", ".join(columns)
                    query = f"""
                        SELECT {group_columns}, COUNT(*) AS duplicate_count 
                        FROM {table}
                        GROUP BY {group_columns}
                        HAVING COUNT(*) > 1
                    """
                    cursor.execute(query)
                    dupes = cursor.fetchall()
                    
                    if dupes:
                        duplicates[table] = {
                            'count': len(dupes),
                            'columns': columns + ['duplicate_count'],
                            'data': dupes
                        }
                
                self.result.emit({'operation': 'duplicates', 'data': duplicates})
                self.progress.emit(100)
                
            conn.close()
            
        except Exception as e:
            self.error.emit(str(e))

class AIWorker(QThread):
    response_received = Signal(str, str)
    error_occurred = Signal(str)

    def __init__(self, messages, model="gpt-4o-mini"):
        super().__init__()
        self.messages = messages
        self.model = model

    def run(self):
        try:
            send = {
                "model": self.model,
                "request": {
                    "messages": self.messages
                }
            }
            
            response = requests.post('http://api.onlysq.ru/ai/v2', json=send)
            
            if response.status_code != 200:
                self.error_occurred.emit(f"HTTP Error {response.status_code}: {response.text}")
                return
                
            response_data = response.json()
            
            if "choices" not in response_data or len(response_data["choices"]) == 0:
                self.error_occurred.emit("Invalid response format from API")
                return
                
            content = response_data["choices"][0]["message"]["content"]
            self.response_received.emit("assistant", content)
            
        except Exception as e:
            self.error_occurred.emit(str(e))

class AIChatTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._parent = parent
        self.txt = parent.txt if parent else t['en']
        self.messages = []
        self.setup_ui()
        
        self.add_message("system", self.txt['ai_system_prompt'])
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        
        splitter = QSplitter(Qt.Vertical)
        
        self.chat_history = QTextBrowser()
        self.chat_history.setStyleSheet("""
            QTextBrowser {
                background-color: transparent;
                color: #e0e0e0;
                border: 1px solid #3a4b6c;
                border-radius: 5px;
                padding: 10px;
                font-size: 12px;
            }
        """)
        self.chat_history.setOpenExternalLinks(True)
        splitter.addWidget(self.chat_history)
        
        input_widget = QWidget()
        input_layout = QVBoxLayout(input_widget)
        input_layout.setContentsMargins(0, 0, 0, 0)
        
        model_layout = QHBoxLayout()
        model_label = QLabel("Model:")
        model_label.setStyleSheet("color: white;")
        self.model_combo = QComboBox()
        
        for model_id, model_name in self.txt['models'].items():
            self.model_combo.addItem(model_name, model_id)
        
        self.model_combo.setStyleSheet("""
            QComboBox {
                background-color: #1e2838;
                color: white;
                border: 1px solid #3a4b6c;
                border-radius: 3px;
                padding: 5px;
            }
        """)
        
        model_layout.addWidget(model_label)
        model_layout.addWidget(self.model_combo)
        model_layout.addStretch()
        input_layout.addLayout(model_layout)
        
        self.message_input = QTextEdit()
        self.message_input.setPlaceholderText(self.txt['enter_message'])
        self.message_input.setStyleSheet("""
            QTextEdit {
                background-color: #1e2838;
                color: white;
                border: 1px solid #3a4b6c;
                border-radius: 5px;
                padding: 10px;
                min-height: 80px;
            }
        """)
        input_layout.addWidget(self.message_input)
        
        button_layout = QHBoxLayout()
        self.send_button = QPushButton(self.txt['send'])
        self.send_button.setStyleSheet(self._parent.button_style if self._parent else "")
        self.send_button.clicked.connect(self.send_message)
        
        self.clear_button = QPushButton(self.txt['clear_chat'])
        self.clear_button.setStyleSheet(self._parent.button_style if self._parent else "")
        self.clear_button.clicked.connect(self.clear_chat)
        
        button_layout.addWidget(self.clear_button)
        button_layout.addStretch()
        button_layout.addWidget(self.send_button)
        input_layout.addLayout(button_layout)
        
        splitter.addWidget(input_widget)
        splitter.setSizes([500, 200])
        
        layout.addWidget(splitter)
        
    def add_message(self, role, content):
        self.messages.append({"role": role, "content": content})
        
        if role == "user":
            prefix = "<b>You:</b>"
            color = "#4a9fcf"
        elif role == "assistant":
            prefix = "<b>Assistant:</b>"
            color = "#7fcf9f"
        else:
            return
            
        content_html = content.replace('\n', '<br>')
        formatted_message = f"""
            <div style="margin-bottom: 15px;">
                <div style="color: {color}; font-weight: bold; margin-bottom: 5px;">
                    {prefix}
                </div>
                <div style="color: #e0e0e0;">
                    {content_html}
                </div>
            </div>
        """
        
        self.chat_history.append(formatted_message)
        self.chat_history.moveCursor(QTextCursor.End)
        
    def send_message(self):
        message = self.message_input.toPlainText().strip()
        if not message:
            return
            
        self.add_message("user", message)
        self.message_input.clear()
        
        self.chat_history.append(f"<div style='color:#aaaaaa; font-style:italic;'>{self.txt['thinking']}</div>")
        self.chat_history.moveCursor(QTextCursor.End)
        
        model_id = self.model_combo.currentData()
        
        messages_to_send = [msg for msg in self.messages if msg["role"] != "system"]
        
        messages_to_send.insert(0, {"role": "system", "content": self.txt['ai_system_prompt']})
        
        self.ai_worker = AIWorker(messages_to_send, model_id)
        self.ai_worker.response_received.connect(self.handle_ai_response)
        self.ai_worker.error_occurred.connect(self.handle_ai_error)
        self.ai_worker.start()
        
    def handle_ai_response(self, role, content):
        self.chat_history.moveCursor(QTextCursor.End)
        self.chat_history.moveCursor(QTextCursor.StartOfLine, QTextCursor.KeepAnchor)
        self.chat_history.textCursor().removeSelectedText()
        self.chat_history.textCursor().deletePreviousChar()
        
        self.add_message(role, content)
        
    def handle_ai_error(self, error):
        self.chat_history.moveCursor(QTextCursor.End)
        self.chat_history.moveCursor(QTextCursor.StartOfLine, QTextCursor.KeepAnchor)
        self.chat_history.textCursor().removeSelectedText()
        self.chat_history.textCursor().deletePreviousChar()
        
        error_message = self.txt['api_error'].format(error=error)
        self.chat_history.append(f"<div style='color:#ff6666;'><b>Error:</b> {error_message}</div>")
        
    def clear_chat(self):
        self.chat_history.clear()
        self.messages = []
        self.add_message("system", self.txt['ai_system_prompt'])
        
    def paintEvent(self, event):
        painter = QPainter(self)
        gradient = QLinearGradient(0, 0, self.width(), self.height())
        gradient.setColorAt(0.0, QColor(30, 40, 60))
        gradient.setColorAt(0.5, QColor(60, 80, 120))
        gradient.setColorAt(1.0, QColor(40, 50, 80))
        painter.fillRect(self.rect(), QBrush(gradient))

class ConverterTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._parent = parent
        self.txt = parent.txt if parent else t['en']
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        layout.setContentsMargins(0, 0, 0, 0)
        
        lang_layout = QHBoxLayout()
        lang_label = QLabel(self.txt['language'])
        lang_label.setStyleSheet("color: white; font-weight: bold;")
        self.lang_cb = QComboBox()
        self.lang_cb.addItems(t.keys())
        self.lang_cb.setCurrentText(self._parent.lang)
        self.lang_cb.currentTextChanged.connect(self._parent.change_lang)
        self.lang_cb.setStyleSheet(self._parent.input_style)
        self.lang_cb.setFixedWidth(100)
        
        lang_layout.addWidget(lang_label)
        lang_layout.addWidget(self.lang_cb)
        lang_layout.addStretch()
        layout.addLayout(lang_layout)
        
        file_group = QGroupBox(self.txt['file_selection'])
        file_group.setStyleSheet(self._parent.input_style)
        file_layout = QVBoxLayout(file_group)
        
        btn_layout = QHBoxLayout()
        self.add_btn = QPushButton(self.txt['add_files'])
        self.clear_btn = QPushButton(self.txt['clear_list'])
        self.merge_db_btn = QPushButton(self.txt['merge_db'])
        
        for btn in [self.add_btn, self.clear_btn, self.merge_db_btn]:
            btn.setStyleSheet(self._parent.button_style)
            btn.setMinimumHeight(35)
        
        self.add_btn.clicked.connect(self._parent.add_files)
        self.clear_btn.clicked.connect(self._parent.clear_list)
        self.merge_db_btn.clicked.connect(self._parent.merge_databases)
        
        btn_layout.addWidget(self.add_btn)
        btn_layout.addWidget(self.clear_btn)
        btn_layout.addWidget(self.merge_db_btn)
        btn_layout.addStretch()
        
        self.file_list = QListWidget()
        self.file_list.setStyleSheet(self._parent.input_style)
        self.file_list.setSelectionMode(QListWidget.ExtendedSelection)
        self.file_list.itemSelectionChanged.connect(self._parent.on_file_selected)
        
        file_layout.addLayout(btn_layout)
        file_layout.addWidget(self.file_list)
        layout.addWidget(file_group)
        
        self.db_group = QGroupBox(self.txt['db_operations'])
        self.db_group.setStyleSheet(self._parent.input_style)
        db_group_layout = QHBoxLayout(self.db_group)
        
        self.db_info_label = QLabel()
        self.db_info_label.setStyleSheet("color: #aaccff; font-weight: bold;")
        self.db_info_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        
        self.open_db_btn = QPushButton(self.txt['open_db'])
        self.open_db_btn.setStyleSheet(self._parent.button_style)
        self.open_db_btn.setMinimumHeight(35)
        self.open_db_btn.clicked.connect(self._parent.open_database)
        self.open_db_btn.setEnabled(False)
        
        self.duplicates_btn = QPushButton(self.txt['find_duplicates'])
        self.duplicates_btn.setStyleSheet(self._parent.button_style)
        self.duplicates_btn.setMinimumHeight(35)
        self.duplicates_btn.clicked.connect(self._parent.find_duplicates)
        self.duplicates_btn.setEnabled(False)
        
        db_group_layout.addWidget(self.db_info_label)
        db_group_layout.addStretch()
        db_group_layout.addWidget(self.open_db_btn)
        db_group_layout.addWidget(self.duplicates_btn)
        
        layout.addWidget(self.db_group)
        
        options_group = QGroupBox(self.txt['options'])
        options_group.setStyleSheet(self._parent.input_style)
        options_layout = QVBoxLayout(options_group)
        
        param_layout = QGridLayout()
        param_layout.setColumnStretch(1, 1)
        
        labels = [
            self.txt['txt_delimiter'],
            self.txt['custom_header'],
            self.txt['row_filter'],
            self.txt['output_name'],
            self.txt['chunk_size']
        ]
        
        self.delim_e = QLineEdit(',')
        self.hdr_e = QLineEdit()
        self.flt_e = QLineEdit()
        self.out_name_e = QLineEdit()
        self.chunk_size_e = QLineEdit('10000')
        
        inputs = [
            self.delim_e,
            self.hdr_e,
            self.flt_e,
            self.out_name_e,
            self.chunk_size_e
        ]
        
        for i, (label_text, input_widget) in enumerate(zip(labels, inputs)):
            label = QLabel(label_text)
            label.setStyleSheet("color: white;")
            param_layout.addWidget(label, i, 0, Qt.AlignLeft)
            param_layout.addWidget(input_widget, i, 1)
            input_widget.setStyleSheet("""
                QLineEdit {
                    background-color: #1e2838;
                    color: white;
                    border: 1px solid #3a4b6c;
                    border-radius: 3px;
                    padding: 5px;
                }
            """)
        
        options_layout.addLayout(param_layout)
        layout.addWidget(options_group)
        
        action_layout = QHBoxLayout()
        self.preview_btn = QPushButton(self.txt['preview'])
        self.convert_btn = QPushButton(self.txt['convert'])
        self.merge_btn = QPushButton(self.txt['convert_merge'])
        
        for btn in [self.preview_btn, self.convert_btn, self.merge_btn]:
            btn.setStyleSheet(self._parent.button_style)
            btn.setMinimumHeight(40)
            btn.setMinimumWidth(150)
        
        self.preview_btn.clicked.connect(self._parent.preview_data)
        self.convert_btn.clicked.connect(partial(self._parent.convert_all, merge=False))
        self.merge_btn.clicked.connect(partial(self._parent.convert_all, merge=True))
        
        action_layout.addWidget(self.preview_btn)
        action_layout.addWidget(self.convert_btn)
        action_layout.addWidget(self.merge_btn)
        layout.addLayout(action_layout)
        
        self.progress = QProgressBar()
        self.progress.setStyleSheet("""
            QProgressBar {
                border: 1px solid #3a4b6c;
                border-radius: 5px;
                background-color: #1e2838;
                text-align: center;
                color: white;
            }
            QProgressBar::chunk {
                background-color: #4a9fcf;
                width: 10px;
            }
        """)
        self.progress.setFixedHeight(20)
        layout.addWidget(self.progress)
        
    def paintEvent(self, event):
        painter = QPainter(self)
        gradient = QLinearGradient(0, 0, self.width(), self.height())
        gradient.setColorAt(0.0, QColor(30, 40, 60))
        gradient.setColorAt(0.5, QColor(60, 80, 120))
        gradient.setColorAt(1.0, QColor(40, 50, 80))
        painter.fillRect(self.rect(), QBrush(gradient))

class DataConverterApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.lang = 'en'
        self.txt = t[self.lang]
        self.files = []
        self.current_db_info = {}
        
        self.setWindowTitle(self.txt['title'])
        self.setMinimumSize(1000, 800)
        
        self.button_style = """
            QPushButton {
                background-color: #3a4b6c;
                color: white;
                border: 1px solid #4a5b7c;
                border-radius: 4px;
                padding: 8px 16px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #4a5b8c;
            }
            QPushButton:pressed {
                background-color: #2a3b5c;
            }
            QPushButton:disabled {
                background-color: #2a3b5c;
                color: #aaaaaa;
            }
        """
        
        self.input_style = """
            QLineEdit, QComboBox, QListWidget {
                background-color: #1e2838;
                color: white;
                border: 1px solid #3a4b6c;
                border-radius: 3px;
                padding: 5px;
            }
            QGroupBox {
                color: #7f9fcf;
                font-weight: bold;
                border: 1px solid #3a4b6c;
                border-radius: 5px;
                margin-top: 1ex;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                subcontrol-position: top center;
                padding: 0 5px;
            }
        """
        
        central_widget = GradientWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(15)
        main_layout.setContentsMargins(20, 20, 20, 20)
        
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget)
        
        self.converter_tab = ConverterTab(self)
        self.tab_widget.addTab(self.converter_tab, self.txt['tabs']['converter'])
        
        self.ai_chat_tab = AIChatTab(self)
        self.tab_widget.addTab(self.ai_chat_tab, self.txt['tabs']['ai_chat'])
        
        self.status_bar = self.statusBar()
        self.status_bar.showMessage("Ready", 5000)
        
        self.db_worker = None

    def change_lang(self, lang):
        if lang not in t:
            return
        self.lang = lang
        self.txt = t[lang]
        self.setWindowTitle(self.txt['title'])
        
        self.tab_widget.setTabText(0, self.txt['tabs']['converter'])
        self.tab_widget.setTabText(1, self.txt['tabs']['ai_chat'])
        
        self.converter_tab.add_btn.setText(self.txt['add_files'])
        self.converter_tab.clear_btn.setText(self.txt['clear_list'])
        self.converter_tab.merge_db_btn.setText(self.txt['merge_db'])
        self.converter_tab.preview_btn.setText(self.txt['preview'])
        self.converter_tab.convert_btn.setText(self.txt['convert'])
        self.converter_tab.merge_btn.setText(self.txt['convert_merge'])
        self.converter_tab.db_group.setTitle(self.txt['db_operations'])
        self.converter_tab.open_db_btn.setText(self.txt['open_db'])
        self.converter_tab.duplicates_btn.setText(self.txt['find_duplicates'])
        
        for widget in self.converter_tab.findChildren(QGroupBox):
            if widget.title() in [t['en']['file_selection'], t['en']['options']]:
                if widget.title() == t['en']['file_selection']:
                    widget.setTitle(self.txt['file_selection'])
                else:
                    widget.setTitle(self.txt['options'])
        
        labels = self.converter_tab.findChildren(QLabel)
        for label in labels:
            text = label.text()
            if text in t['en'].values():
                for key, value in t['en'].items():
                    if value == text:
                        label.setText(self.txt[key])
                        break
        
        if hasattr(self, 'current_db_path'):
            self.on_file_selected()
        
        self.ai_chat_tab.txt = self.txt
        self.ai_chat_tab.message_input.setPlaceholderText(self.txt['enter_message'])
        self.ai_chat_tab.send_button.setText(self.txt['send'])
        self.ai_chat_tab.clear_button.setText(self.txt['clear_chat'])
        
        self.ai_chat_tab.model_combo.clear()
        for model_id, model_name in self.txt['models'].items():
            self.ai_chat_tab.model_combo.addItem(model_name, model_id)
        
        self.ai_chat_tab.messages = [msg for msg in self.ai_chat_tab.messages if msg["role"] != "system"]
        self.ai_chat_tab.add_message("system", self.txt['ai_system_prompt'])

    def add_files(self):
        file_dialog = QFileDialog()
        file_dialog.setFileMode(QFileDialog.ExistingFiles)
        file_dialog.setNameFilters([
            "Databases (*.db *.sqlite *.sql)",
            "Excel (*.xls *.xlsx)",
            "Text (*.txt *.csv)",
            "JSON (*.json)"
        ])
        if file_dialog.exec():
            paths = file_dialog.selectedFiles()
            for p in paths:
                if p not in self.files:
                    self.files.append(p)
                    self.converter_tab.file_list.addItem(p)

    def clear_list(self):
        self.files.clear()
        self.converter_tab.file_list.clear()
        self.converter_tab.db_info_label.clear()
        self.converter_tab.open_db_btn.setEnabled(False)
        self.converter_tab.duplicates_btn.setEnabled(False)

    def on_file_selected(self):
        selected = self.converter_tab.file_list.selectedItems()
        if not selected or len(selected) > 1:
            self.converter_tab.db_info_label.clear()
            self.converter_tab.open_db_btn.setEnabled(False)
            self.converter_tab.duplicates_btn.setEnabled(False)
            return
            
        path = selected[0].text()
        ext = os.path.splitext(path)[1].lower()
        
        if ext in ['.db', '.sqlite', '.sql']:
            self.current_db_path = path
            self.converter_tab.open_db_btn.setEnabled(True)
            self.converter_tab.duplicates_btn.setEnabled(True)
            
            if path in self.current_db_info:
                info = self.current_db_info[path]
                self.show_db_info(info)
                return
                
            if self.db_worker and self.db_worker.isRunning():
                self.db_worker.terminate()
                
            self.db_worker = DatabaseWorker(path, 'info')
            self.db_worker.result.connect(self.handle_db_info)
            self.db_worker.error.connect(self.handle_db_error)
            self.db_worker.start()
            
            self.converter_tab.db_info_label.setText(self.txt['db_info_short'].format(tables="..."))
        else:
            df = self.load_dataframe(path, preview=True)
            if df is not None:
                info_text = f"Rows: {len(df)}, Columns: {len(df.columns)}"
                self.converter_tab.db_info_label.setText(info_text)
            else:
                self.converter_tab.db_info_label.clear()
            self.converter_tab.open_db_btn.setEnabled(False)
            self.converter_tab.duplicates_btn.setEnabled(False)

    def handle_db_info(self, result):
        if result['operation'] == 'info':
            self.current_db_info[self.current_db_path] = result
            self.show_db_info(result)
        elif result['operation'] == 'duplicates':
            self.show_duplicates(result)

    def handle_db_error(self, error_msg):
        QMessageBox.critical(self, self.txt['error'], f"Database error:\n{error_msg}")
        self.converter_tab.db_info_label.setText("Error loading DB info")

    def show_db_info(self, db_info):
        if not db_info or 'tables' not in db_info:
            return
            
        tables = db_info['tables']
        table_info = db_info.get('table_info', {})
        
        if tables:
            info_text = f"{self.txt['db_info_short'].format(tables=len(tables))}"
            
            first_table = tables[0]
            if first_table in table_info:
                t_info = table_info[first_table]
                info_text += " | " + self.txt['table_info'].format(
                    table=first_table,
                    rows=t_info['rows'],
                    cols=t_info['cols']
                )
            
            self.converter_tab.db_info_label.setText(info_text)
        else:
            self.converter_tab.db_info_label.setText(self.txt['db_info_no_table'].format(tables=0))

    def open_database(self):
        selected = self.converter_tab.file_list.selectedItems()
        if not selected:
            return
        path = selected[0].text()
        
        if path not in self.current_db_info:
            QMessageBox.warning(self, self.txt['info'], "Database info not loaded yet")
            return
            
        db_info = self.current_db_info[path]
        tables = db_info['tables']
        
        if not tables:
            QMessageBox.information(self, self.txt['info'], "No tables found")
            return
            
        table, ok = QInputDialog.getItem(
            self, 
            self.txt['title'], 
            self.txt['select_table_for_open'], 
            tables, 
            0, False
        )
        if not ok or not table:
            return
            
        try:
            conn = sqlite3.connect(path)
            query = f"SELECT * FROM {table} LIMIT 1000"
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            preview_dialog = PreviewDialog(
                f"Database: {os.path.basename(path)} - Table: {table}",
                df,
                self,
                full_table=True
            )
            preview_dialog.exec()
            
        except Exception as e:
            QMessageBox.critical(self, self.txt['error'], str(e))

    def find_duplicates(self):
        selected = self.converter_tab.file_list.selectedItems()
        if not selected:
            return
        path = selected[0].text()
        
        if self.db_worker and self.db_worker.isRunning():
            self.db_worker.terminate()
            
        self.db_worker = DatabaseWorker(path, 'duplicates')
        self.db_worker.progress.connect(self.converter_tab.progress.setValue)
        self.db_worker.result.connect(self.handle_db_info)
        self.db_worker.error.connect(self.handle_db_error)
        self.db_worker.start()
        
        self.converter_tab.progress.setRange(0, 100)
        self.converter_tab.progress.setValue(0)
        self.status_bar.showMessage("Searching for duplicates...")

    def show_duplicates(self, result):
        if result['operation'] != 'duplicates':
            return
            
        duplicates = result['data']
        if not duplicates:
            QMessageBox.information(self, self.txt['info'], self.txt['no_duplicates'])
            self.status_bar.showMessage("No duplicates found", 5000)
            return
            
        dialog = QDialog(self)
        dialog.setWindowTitle(self.txt['find_duplicates'])
        dialog.setMinimumSize(600, 400)
        
        layout = QVBoxLayout(dialog)
        
        table_label = QLabel("Select table with duplicates:")
        table_label.setStyleSheet("color: white;")
        layout.addWidget(table_label)
        
        table_combo = QComboBox()
        table_combo.addItems(duplicates.keys())
        table_combo.setStyleSheet(self.input_style)
        layout.addWidget(table_combo)
        
        view_btn = QPushButton("View Duplicates")
        view_btn.setStyleSheet(self.button_style)
        layout.addWidget(view_btn)
        
        def show_duplicates_table():
            table_name = table_combo.currentText()
            dup_info = duplicates[table_name]
            
            df = pd.DataFrame(dup_info['data'], columns=dup_info['columns'])
            
            dup_dialog = PreviewDialog(
                f"Duplicates in {table_name}",
                df,
                self
            )
            dup_dialog.exec()
        
        view_btn.clicked.connect(show_duplicates_table)
        
        count_label = QLabel(f"Found duplicates in {len(duplicates)} tables")
        count_label.setStyleSheet("color: #aaffaa; font-weight: bold;")
        layout.addWidget(count_label)
        
        dialog.exec()
        self.status_bar.showMessage(self.txt['duplicates_found'].format(count=len(duplicates)), 5000)

    def merge_databases(self):
        db_files, _ = QFileDialog.getOpenFileNames(
            self,
            self.txt['merge_title'],
            "",
            "Database files (*.db *.sqlite *.sql)"
        )
        if not db_files:
            return

        output_file, _ = QFileDialog.getSaveFileName(
            self,
            self.txt['select_output'],
            "",
            "SQLite Database (*.db)"
        )
        if not output_file:
            return

        conn = sqlite3.connect(db_files[0])
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn).name.tolist()
        conn.close()

        merge_dialog = MergeDialog(self.txt['merge_title'], tables, self)
        if merge_dialog.exec() != QDialog.Accepted:
            return

        selected_tables = merge_dialog.get_selected_tables()
        if not selected_tables:
            QMessageBox.warning(self, self.txt['title'], "No tables selected!")
            return

        try:
            new_conn = sqlite3.connect(output_file)

            for db_file in db_files:
                source_conn = sqlite3.connect(db_file)

                for table in selected_tables:
                    try:
                        df = pd.read_sql_query(f"SELECT * FROM {table}", source_conn)

                        if db_file != db_files[0]:
                            try:
                                existing = pd.read_sql_query(f"SELECT * FROM {table}", new_conn)
                                df = pd.concat([existing, df], ignore_index=True)
                            except:
                                pass

                        df.to_sql(table, new_conn, if_exists='replace', index=False)

                    except sqlite3.OperationalError as e:
                        if "no such table" in str(e):
                            continue
                        raise

                source_conn.close()

            new_conn.close()
            QMessageBox.information(self, self.txt['title'], self.txt['merge_done'])
            self.status_bar.showMessage("Databases merged successfully", 5000)

        except Exception as e:
            QMessageBox.critical(self, self.txt['title'], f"Error merging databases:\n\n{str(e)}")
            if 'new_conn' in locals():
                new_conn.close()

    def preview_data(self):
        selected = self.converter_tab.file_list.selectedItems()
        if not selected:
            QMessageBox.warning(self, self.txt['title'], self.txt['no_selection'])
            return
        path = selected[0].text()
        df = self.load_dataframe(path, preview=True)
        if df is None:
            return
        
        preview_dialog = PreviewDialog(
            self.txt['preview_title'].format(os.path.basename(path)), 
            df.head(100),
            self
        )
        preview_dialog.exec()

    def load_dataframe(self, path, preview=False):
        ext = os.path.splitext(path)[1].lower()
        try:
            if ext in ['.xls', '.xlsx']:
                sheets = pd.ExcelFile(path).sheet_names
                if len(sheets) == 1:
                    sheet = sheets[0]
                else:
                    sheet, ok = QInputDialog.getText(
                        self, 
                        self.txt['title'], 
                        self.txt['select_sheet'].format(sheets=sheets)
                    )
                    if not ok or not sheet:
                        return None
                
                read_kwargs = {'sheet_name': sheet}
                if preview:
                    read_kwargs['nrows'] = 1000
                df = pd.read_excel(path, **read_kwargs)

            elif ext in ['.txt', '.csv']:
                read_kwargs = {'delimiter': self.converter_tab.delim_e.text() or ',',
                            'engine': 'python',
                            'on_bad_lines': 'skip'}
                if preview:
                    read_kwargs['nrows'] = 1000
                try:
                    df = pd.read_csv(path, **read_kwargs)
                except pd.errors.ParserError as e:
                    if ";' expected after \"'" in str(e):
                        with open(path, 'r', encoding='utf-8') as f:
                            lines = f.readlines()
                        error_line = None
                        for i, line in enumerate(lines, 1):
                            if '"' in line and ';' not in line.split('" читать')[-1]:
                                error_line = f"Line {i}: {line.strip()}"
                                break
                        error_msg = f"CSV parsing error:\n\n{str(e)}\n\nProblematic line:\n{error_line if error_line else 'Not found'}"
                        QMessageBox.critical(self, self.txt['title'], error_msg)
                    else:
                        QMessageBox.critical(self, self.txt['title'], f"CSV parsing error:\n\n{str(e)}")
                    return None

            elif ext == '.json':
                orient, ok = QInputDialog.getText(
                    self, 
                    self.txt['title'], 
                    self.txt['json_orient'], 
                    text='records'
                )
                if not ok or not orient:
                    return None
                df = pd.read_json(path, orient=orient)
                if preview:
                    df = df.head(1000)

            elif ext in ['.db', '.sqlite']:
                conn = sqlite3.connect(path)
                tabs = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn).name.tolist()
                tbl, ok = QInputDialog.getText(
                    self, 
                    self.txt['title'], 
                    self.txt['select_table'].format(tables=tabs)
                )
                if not ok or not tbl:
                    conn.close()
                    return None
                query = f"SELECT * FROM {tbl}"
                if preview:
                    query += " LIMIT 1000"
                df = pd.read_sql_query(query, conn)
                conn.close()

            elif ext == '.sql':
                sql_text = open(path, 'r', encoding='utf-8').read()
                conn = sqlite3.connect(':memory:')
                conn.executescript(sql_text)
                tabs = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn).name.tolist()
                tbl, ok = QInputDialog.getText(
                    self, 
                    self.txt['title'], 
                    self.txt['select_table'].format(tables=tabs)
                )
                if not ok or not tbl:
                    conn.close()
                    return None
                query = f"SELECT * FROM {tbl}"
                if preview:
                    query += " LIMIT 1000"
                df = pd.read_sql_query(query, conn)
                conn.close()

            else:
                QMessageBox.critical(self, self.txt['title'], f"{self.txt['unsupported']} {ext}")
                return None

            flt = self.converter_tab.flt_e.text().strip()
            if flt:
                try:
                    df = df.query(flt)
                except Exception as e:
                    QMessageBox.critical(self, self.txt['title'], f"Filter error:\n\n{str(e)}")
                    return None
            hdr = self.converter_tab.hdr_e.text().strip()
            if hdr:
                cols = [h.strip() for h in hdr.split(',')]
                if len(cols) == len(df.columns):
                    df.columns = cols

            return df

        except Exception as e:
            QMessageBox.critical(self, self.txt['title'], str(e))
            return None

    def convert_all(self, merge=False):
        if not self.files:
            QMessageBox.warning(self, self.txt['title'], self.txt['no_files'])
            return

        selected = [item.text() for item in self.converter_tab.file_list.selectedItems()]
        files_to_process = selected if selected else self.files

        if merge:
            output_path, _ = QFileDialog.getSaveFileName(
                self,
                self.txt['select_output'],
                "",
                "CSV files (*.csv);;Excel files (*.xlsx)"
            )
            if not output_path:
                return

            self.converter_tab.progress.setRange(0, 100)
            self.converter_tab.progress.setValue(0)

            if self.merge_large_files(files_to_process, output_path):
                QMessageBox.information(self, self.txt['info'], self.txt['merge_done'])
                self.status_bar.showMessage("Files merged successfully", 5000)
        else:
            out_dir = QFileDialog.getExistingDirectory(
                self, 
                self.txt['select_output']
            )
            if not out_dir:
                return

            self.converter_tab.progress.setRange(0, len(files_to_process))
            self.converter_tab.progress.setValue(0)

            for i, path in enumerate(files_to_process, 1):
                try:
                    ext = os.path.splitext(path)[1].lower()
                    output_name = self.converter_tab.out_name_e.text().strip()

                    if not output_name:
                        base_name = os.path.splitext(os.path.basename(path))[0]
                    else:
                        base_name = output_name
                        if len(files_to_process) > 1:
                            base_name = f"{base_name}_{i}"

                    output_path = os.path.join(out_dir, f"{base_name}.csv")

                    if ext in ['.csv', '.txt']:
                        first_chunk = True
                        chunk_size = int(self.converter_tab.chunk_size_e.text() or 10000)
                        for chunk in pd.read_csv(
                                path,
                                delimiter=self.converter_tab.delim_e.text() or ',',
                                chunksize=chunk_size,
                                engine='python',
                                on_bad_lines='skip'
                            ):
                            if first_chunk:
                                chunk.to_csv(output_path, index=False)
                                first_chunk = False
                            else:
                                chunk.to_csv(output_path, mode='a', index=False, header=False)
                    else:
                        df = self.load_dataframe(path)
                        if df is not None:
                            df.to_csv(output_path, index=False)

                except Exception as e:
                    QMessageBox.critical(self, self.txt['error'], f"Error processing {path}:\n{str(e)}")

                self.converter_tab.progress.setValue(i)
                self.status_bar.showMessage(f"Processing {i}/{len(files_to_process)} files...")
                QApplication.processEvents()

            self.status_bar.showMessage(self.txt['done'], 5000)
            QMessageBox.information(self, self.txt['info'], self.txt['done'])

    def merge_large_files(self, input_files, output_path):
        try:
            chunk_size = int(self.converter_tab.chunk_size_e.text() or 10000)
            first_write = True
            to_excel = output_path.lower().endswith('.xlsx')
            
            if not to_excel:
                f_out = open(output_path, 'w', newline='', encoding='utf-8')

            total_files = len(input_files)
            processed = 0
            
            for path in input_files:
                ext = os.path.splitext(path)[1].lower()
                
                if ext in ['.csv', '.txt']:
                    for chunk in pd.read_csv(
                            path,
                            delimiter=self.converter_tab.delim_e.text() or ',',
                            chunksize=chunk_size,
                            engine='python',
                            on_bad_lines='skip'
                        ):
                        if to_excel:
                            if first_write:
                                all_chunks = [chunk]
                                first_write = False
                            else:
                                all_chunks.append(chunk)
                        else:
                            if first_write:
                                chunk.to_csv(f_out, index=False)
                                first_write = False
                            else:
                                chunk.to_csv(f_out, index=False, header=False, mode='a')
                
                elif ext in ['.xls', '.xlsx']:
                    try:
                        df = pd.read_excel(path)
                        if to_excel:
                            if first_write:
                                all_chunks = [df]
                                first_write = False
                            else:
                                all_chunks.append(df)
                        else:
                            if first_write:
                                df.to_csv(f_out, index=False)
                                first_write = False
                            else:
                                df.to_csv(f_out, index=False, header=False, mode='a')
                    except Exception as e:
                        QMessageBox.warning(self, self.txt['error'], 
                                          f"Ошибка при чтении Excel файла {path}:\n{str(e)}")
                        continue
                
                elif ext == '.json':
                    try:
                        df = pd.read_json(path)
                        if to_excel:
                            if first_write:
                                all_chunks = [df]
                                first_write = False
                            else:
                                all_chunks.append(df)
                        else:
                            if first_write:
                                df.to_csv(f_out, index=False)
                                first_write = False
                            else:
                                df.to_csv(f_out, index=False, header=False, mode='a')
                    except Exception as e:
                        QMessageBox.warning(self, self.txt['error'], 
                                          f"Ошибка при чтении JSON файла {path}:\n{str(e)}")
                        continue
                
                processed += 1
                progress = int((processed / total_files) * 100)
                self.converter_tab.progress.setValue(progress)
                self.status_bar.showMessage(f"Обработка {processed}/{total_files} файлов...")
                QApplication.processEvents()

            if not to_excel:
                f_out.close()
            else:
                try:
                    merged_df = pd.concat(all_chunks, ignore_index=True)
                    merged_df.to_excel(output_path, index=False)
                except Exception as e:
                    QMessageBox.critical(self, self.txt['error'], 
                                       f"Ошибка при сохранении Excel:\n{str(e)}")
                    return False

            return True

        except Exception as e:
            QMessageBox.critical(self, self.txt['error'], f"Ошибка при объединении файлов:\n{str(e)}")
            if 'f_out' in locals() and not to_excel:
                f_out.close()
            return False

if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setStyle(QStyleFactory.create('Fusion'))
    
    palette = app.palette()
    palette.setColor(palette.ColorRole.Window, QColor(30, 40, 60))
    palette.setColor(palette.ColorRole.WindowText, Qt.GlobalColor.white)
    palette.setColor(palette.ColorRole.Base, QColor(25, 35, 50))
    palette.setColor(palette.ColorRole.AlternateBase, QColor(35, 45, 60))
    palette.setColor(palette.ColorRole.ToolTipBase, Qt.GlobalColor.white)
    palette.setColor(palette.ColorRole.ToolTipText, Qt.GlobalColor.white)
    palette.setColor(palette.ColorRole.Text, Qt.GlobalColor.white)
    palette.setColor(palette.ColorRole.Button, QColor(50, 60, 80))
    palette.setColor(palette.ColorRole.ButtonText, Qt.GlobalColor.white)
    palette.setColor(palette.ColorRole.BrightText, Qt.GlobalColor.red)
    palette.setColor(palette.ColorRole.Highlight, QColor(70, 100, 150))
    palette.setColor(palette.ColorRole.HighlightedText, Qt.GlobalColor.white)
    app.setPalette(palette)
    
    window = DataConverterApp()
    window.show()
    sys.exit(app.exec())