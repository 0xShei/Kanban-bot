import sqlite3
from datetime import datetime
import os


class KanbanDatabase:
    """A class to manage Kanban task operations with SQLite database."""
    
    VALID_STATUSES = ('todo', 'doing', 'done')
    
    def __init__(self, db_name='kanban.db'):
        self.db_name = db_name
        self.initialize_database()
    
    def get_connection(self):
        return sqlite3.connect(self.db_name)
    
    def initialize_database(self):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    status TEXT NOT NULL CHECK(status IN ('todo', 'doing', 'done')),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    priority INTEGER DEFAULT 0,
                    due_date DATETIME
                )
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tasks_user_id ON tasks(user_id)
            ''')
            
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Database initialization error: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def add_task(self, user_id, title, description='', status='todo', priority=0, due_date=None):
        if not title.strip():
            print("Error: Task title cannot be empty.")
            return None
            
        if status not in self.VALID_STATUSES:
            print(f"Error: Status must be one of {self.VALID_STATUSES}")
            return None
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO tasks (user_id, title, description, status, priority, due_date)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, title, description, status, priority, due_date))
            
            task_id = cursor.lastrowid
            conn.commit()
            return task_id
        except sqlite3.Error as e:
            print(f"Error adding task: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def update_task_status(self, task_id, new_status, user_id=None):
        if new_status not in self.VALID_STATUSES:
            print(f"Error: Status must be one of {self.VALID_STATUSES}")
            return False
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if user_id:
                cursor.execute('''
                    UPDATE tasks
                    SET status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND user_id = ?
                ''', (new_status, task_id, user_id))
            else:
                cursor.execute('''
                    UPDATE tasks
                    SET status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (new_status, task_id))
            
            if cursor.rowcount == 0:
                return False
                
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error updating task status: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def update_task(self, task_id, user_id=None, **kwargs):
        if 'status' in kwargs and kwargs['status'] not in self.VALID_STATUSES:
            print(f"Error: Status must be one of {self.VALID_STATUSES}")
            return False
            
        if not kwargs:
            print("Error: No update parameters provided.")
            return False
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            set_clause = ', '.join([f"{field} = ?" for field in kwargs.keys()])
            set_clause += ", updated_at = CURRENT_TIMESTAMP"
            
            where_clause = "WHERE id = ?"
            params = list(kwargs.values()) + [task_id]
            
            if user_id:
                where_clause += " AND user_id = ?"
                params.append(user_id)
            
            cursor.execute(f"UPDATE tasks SET {set_clause} {where_clause}", params)
            
            if cursor.rowcount == 0:
                return False
                
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error updating task: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def delete_task(self, task_id, user_id=None):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if user_id:
                cursor.execute('''
                    DELETE FROM tasks
                    WHERE id = ? AND user_id = ?
                ''', (task_id, user_id))
            else:
                cursor.execute('''
                    DELETE FROM tasks
                    WHERE id = ?
                ''', (task_id,))
            
            if cursor.rowcount == 0:
                return False
                
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error deleting task: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def list_tasks_by_user(self, user_id, status_filter=None, sort_by='created_at', order='ASC'):
        valid_sort_fields = ['id', 'title', 'status', 'created_at', 'updated_at', 'priority', 'due_date']
        if sort_by not in valid_sort_fields:
            sort_by = 'created_at'
            
        if order not in ['ASC', 'DESC']:
            order = 'ASC'
            
        if status_filter and status_filter not in self.VALID_STATUSES:
            status_filter = None
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = f'''
                SELECT id, title, description, status, priority, due_date, created_at, updated_at
                FROM tasks
                WHERE user_id = ?
                {f"AND status = ?" if status_filter else ""}
                ORDER BY {sort_by} {order}
            '''
            
            params = [user_id]
            if status_filter:
                params.append(status_filter)
                
            cursor.execute(query, params)
            tasks = cursor.fetchall()
            
            columns = ['id', 'title', 'description', 'status', 'priority', 'due_date', 'created_at', 'updated_at']
            task_list = [dict(zip(columns, task)) for task in tasks]
            
            return task_list
        except sqlite3.Error as e:
            print(f"Error listing tasks: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_task_counts_by_status(self, user_id):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT status, COUNT(*) as count
                FROM tasks
                WHERE user_id = ?
                GROUP BY status
            ''', (user_id,))
            
            results = cursor.fetchall()
            
            counts = {status: 0 for status in self.VALID_STATUSES}
            
            for status, count in results:
                counts[status] = count
                
            return counts
        except sqlite3.Error as e:
            print(f"Error getting task counts: {e}")
            return {status: 0 for status in self.VALID_STATUSES}
        finally:
            if conn:
                conn.close()
    
    def backup_database(self, backup_dir="backups"):
        try:
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"{os.path.splitext(self.db_name)[0]}_{timestamp}.db"
            backup_path = os.path.join(backup_dir, backup_filename)
            
            conn = self.get_connection()
            
            backup_conn = sqlite3.connect(backup_path)
            conn.backup(backup_conn)
            
            backup_conn.close()
            conn.close()
            
            return backup_path
        except (sqlite3.Error, OSError) as e:
            print(f"Error backing up database: {e}")
            return None