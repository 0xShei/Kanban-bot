import sqlite3
from datetime import datetime
import os


class KanbanDatabase: 
    VALID_STATUSES = ('todo', 'doing', 'done')
    BOARD_TYPES = ('personal', 'public')
    
    def __init__(self, db_name='kanban.db'):
        self.db_name = db_name
        self.initialize_database()
    
    def get_connection(self):
        return sqlite3.connect(self.db_name)
    
    def initialize_database(self):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Boards Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS boards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    board_type TEXT NOT NULL CHECK(board_type IN ('personal', 'public')),
                    owner_id TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tasks Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    board_id INTEGER NOT NULL,
                    user_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    status TEXT NOT NULL CHECK(status IN ('todo', 'doing', 'done')),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    priority INTEGER DEFAULT 0,
                    due_date DATETIME,
                    FOREIGN KEY (board_id) REFERENCES boards(id) ON DELETE CASCADE
                )
            ''')
            
            #  Admin roles check table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS admin_roles (
                    user_id TEXT PRIMARY KEY,
                    is_admin BOOLEAN NOT NULL DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Indexes
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tasks_user_id ON tasks(user_id)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tasks_board_id ON tasks(board_id)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_boards_owner_id ON boards(owner_id)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_boards_type ON boards(board_type)
            ''')
            
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Database initialization error: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def delete_task(self, task_id, user_id):
        """Delete a task if user has access to the board"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # First get task and board details
            cursor.execute('''
                SELECT t.board_id, t.user_id, b.owner_id, b.board_type
                FROM tasks t
                JOIN boards b ON t.board_id = b.id
                WHERE t.id = ?
            ''', (task_id,))
            
            result = cursor.fetchone()
            if not result:
                print(f"Error: Task with ID {task_id} not found.")
                return False
                
            board_id, task_owner_id, board_owner_id, board_type = result
            
            # Check permissions
            is_task_owner = user_id == task_owner_id
            is_board_owner = user_id == board_owner_id
            is_admin_on_public = board_type == 'public' and self.is_admin(user_id)
            
            has_permission = is_task_owner or is_board_owner or is_admin_on_public
            
            if not has_permission:
                print(f"Error: User {user_id} does not have permission to delete task {task_id}")
                return False
            
            # Delete the task
            cursor.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error deleting task: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def list_tasks_by_board(self, board_id, user_id, status_filter=None, sort_by='created_at', order='ASC'):
        """List tasks for a specific board if the user has access"""
        valid_sort_fields = ['id', 'title', 'status', 'created_at', 'updated_at', 'priority', 'due_date', 'user_id']
        if sort_by not in valid_sort_fields:
            sort_by = 'created_at'
            
        if order not in ['ASC', 'DESC']:
            order = 'ASC'
            
        if status_filter and status_filter not in self.VALID_STATUSES:
            status_filter = None
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # First check if user has access to this board
            cursor.execute('''
                SELECT owner_id, board_type FROM boards WHERE id = ?
            ''', (board_id,))
            
            result = cursor.fetchone()
            if not result:
                print(f"Error: Board with ID {board_id} not found.")
                return []
                
            owner_id, board_type = result
            
            # Check access permissions
            has_access = (owner_id == user_id) or (board_type == 'public')
            if not has_access:
                print(f"Error: User {user_id} does not have access to board {board_id}")
                return []
            
            # Build query for tasks
            query = f'''
                SELECT id, board_id, user_id, title, description, status, priority, due_date, created_at, updated_at
                FROM tasks
                WHERE board_id = ?
                {f"AND status = ?" if status_filter else ""}
                ORDER BY {sort_by} {order}
            '''
            
            params = [board_id]
            if status_filter:
                params.append(status_filter)
                
            cursor.execute(query, params)
            tasks = cursor.fetchall()
            
            columns = ['id', 'board_id', 'user_id', 'title', 'description', 'status', 'priority', 'due_date', 'created_at', 'updated_at']
            task_list = [dict(zip(columns, task)) for task in tasks]
            
            return task_list
        except sqlite3.Error as e:
            print(f"Error listing tasks: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_task_counts_by_board(self, board_id, user_id):
        """Get task counts by status for a board if the user has access"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # First check if user has access to this board
            cursor.execute('''
                SELECT owner_id, board_type FROM boards WHERE id = ?
            ''', (board_id,))
            
            result = cursor.fetchone()
            if not result:
                print(f"Error: Board with ID {board_id} not found.")
                return {status: 0 for status in self.VALID_STATUSES}
                
            owner_id, board_type = result
            
            # Check access permissions
            has_access = (owner_id == user_id) or (board_type == 'public')
            if not has_access:
                print(f"Error: User {user_id} does not have access to board {board_id}")
                return {status: 0 for status in self.VALID_STATUSES}
            
            # Get task counts
            cursor.execute('''
                SELECT status, COUNT(*) as count
                FROM tasks
                WHERE board_id = ?
                GROUP BY status
            ''', (board_id,))
            
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
    
    # Board management methods
    def create_board(self, name, owner_id, description='', board_type='personal'):
        """Create a new Kanban board"""
        if not name.strip():
            print("Error: Board name cannot be empty.")
            return None
            
        if board_type not in self.BOARD_TYPES:
            print(f"Error: Board type must be one of {self.BOARD_TYPES}")
            return None
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO boards (name, owner_id, description, board_type)
                VALUES (?, ?, ?, ?)
            ''', (name, owner_id, description, board_type))
            
            board_id = cursor.lastrowid
            conn.commit()
            return board_id
        except sqlite3.Error as e:
            print(f"Error creating board: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def update_board(self, board_id, user_id, **kwargs):
        """Update board details if user is owner or admin"""
        if 'board_type' in kwargs and kwargs['board_type'] not in self.BOARD_TYPES:
            print(f"Error: Board type must be one of {self.BOARD_TYPES}")
            return False
            
        if not kwargs:
            print("Error: No update parameters provided.")
            return False
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # First check if user is owner or admin
            cursor.execute('''
                SELECT owner_id, board_type FROM boards WHERE id = ?
            ''', (board_id,))
            
            result = cursor.fetchone()
            if not result:
                print(f"Error: Board with ID {board_id} not found.")
                return False
                
            owner_id, board_type = result
            
            # Check permissions
            has_permission = (owner_id == user_id) or (board_type == 'public' and self.is_admin(user_id))
            if not has_permission:
                print(f"Error: User {user_id} does not have permission to update board {board_id}")
                return False
            
            # Update the board
            set_clause = ', '.join([f"{field} = ?" for field in kwargs.keys()])
            set_clause += ", updated_at = CURRENT_TIMESTAMP"
            
            cursor.execute(f"UPDATE boards SET {set_clause} WHERE id = ?", 
                list(kwargs.values()) + [board_id])
            
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error updating board: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def delete_board(self, board_id, user_id):
        """Delete a board if user is owner or admin"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # First check if user is owner or admin
            cursor.execute('''
                SELECT owner_id, board_type FROM boards WHERE id = ?
            ''', (board_id,))
            
            result = cursor.fetchone()
            if not result:
                print(f"Error: Board with ID {board_id} not found.")
                return False
                
            owner_id, board_type = result
            
            # Check permissions
            has_permission = (owner_id == user_id) or (board_type == 'public' and self.is_admin(user_id))
            if not has_permission:
                print(f"Error: User {user_id} does not have permission to delete board {board_id}")
                return False
            
            # Delete the board - cascade will delete all tasks
            cursor.execute("DELETE FROM boards WHERE id = ?", (board_id,))
            
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error deleting board: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def list_boards_for_user(self, user_id):
        """List all boards a user can access (personal + public)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get user's personal boards and all public boards
            cursor.execute('''
                SELECT id, name, description, board_type, owner_id, created_at, updated_at
                FROM boards
                WHERE owner_id = ? OR board_type = 'public'
                ORDER BY board_type, name
            ''', (user_id,))
            
            boards = cursor.fetchall()
            
            columns = ['id', 'name', 'description', 'board_type', 'owner_id', 'created_at', 'updated_at']
            board_list = [dict(zip(columns, board)) for board in boards]
            
            return board_list
        except sqlite3.Error as e:
            print(f"Error listing boards: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_board_details(self, board_id, user_id):
        """Get board details if user has access"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, name, description, board_type, owner_id, created_at, updated_at
                FROM boards
                WHERE id = ?
            ''', (board_id,))
            
            result = cursor.fetchone()
            if not result:
                print(f"Error: Board with ID {board_id} not found.")
                return None
                
            columns = ['id', 'name', 'description', 'board_type', 'owner_id', 'created_at', 'updated_at']
            board = dict(zip(columns, result))
            
            # Check if user has access to this board
            has_access = (board['owner_id'] == user_id) or (board['board_type'] == 'public')
            if not has_access:
                print(f"Error: User {user_id} does not have access to board {board_id}")
                return None
            
            return board
        except sqlite3.Error as e:
            print(f"Error getting board details: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    # Admin management methods
    def set_admin(self, user_id, is_admin=True):
        """Set a user's admin status"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO admin_roles (user_id, is_admin)
                VALUES (?, ?)
            ''', (user_id, is_admin))
            
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error setting admin status: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def remove_admin(self, user_id):
        """Remove admin privileges from a user"""
        return self.set_admin(user_id, False)
    
    def is_admin(self, user_id):
        """Check if a user has admin privileges"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT is_admin FROM admin_roles WHERE user_id = ?
            ''', (user_id,))
            
            result = cursor.fetchone()
            return result and result[0]
        except sqlite3.Error as e:
            print(f"Error checking admin status: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    # Enhanced task methods with board support
    def add_task(self, board_id, user_id, title, description='', status='todo', priority=0, due_date=None):
        """Add a task to a specific board if the user has access"""
        if not title.strip():
            print("Error: Task title cannot be empty.")
            return None
            
        if status not in self.VALID_STATUSES:
            print(f"Error: Status must be one of {self.VALID_STATUSES}")
            return None
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # First check if user has access to this board
            cursor.execute('''
                SELECT owner_id, board_type FROM boards WHERE id = ?
            ''', (board_id,))
            
            result = cursor.fetchone()
            if not result:
                print(f"Error: Board with ID {board_id} not found.")
                return None
                
            owner_id, board_type = result
            
            # Check permissions
            has_permission = (owner_id == user_id) or (board_type == 'public' and self.is_admin(user_id))
            if not has_permission:
                print(f"Error: User {user_id} does not have permission to add tasks to board {board_id}")
                return None
            
            # Add the task
            cursor.execute('''
                INSERT INTO tasks (board_id, user_id, title, description, status, priority, due_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (board_id, user_id, title, description, status, priority, due_date))
            
            task_id = cursor.lastrowid
            conn.commit()
            return task_id
        except sqlite3.Error as e:
            print(f"Error adding task: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def update_task_status(self, task_id, new_status, user_id):
        """Update a task's status if user has access to the board"""
        if new_status not in self.VALID_STATUSES:
            print(f"Error: Status must be one of {self.VALID_STATUSES}")
            return False
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # First get task and board details
            cursor.execute('''
                SELECT t.board_id, t.user_id, b.owner_id, b.board_type
                FROM tasks t
                JOIN boards b ON t.board_id = b.id
                WHERE t.id = ?
            ''', (task_id,))
            
            result = cursor.fetchone()
            if not result:
                print(f"Error: Task with ID {task_id} not found.")
                return False
                
            board_id, task_owner_id, board_owner_id, board_type = result
            
            # Check permissions
            is_task_owner = user_id == task_owner_id
            is_board_owner = user_id == board_owner_id
            is_admin_on_public = board_type == 'public' and self.is_admin(user_id)
            
            has_permission = is_task_owner or is_board_owner or is_admin_on_public
            
            if not has_permission:
                print(f"Error: User {user_id} does not have permission to update task {task_id}")
                return False
            
            # Update the task status
            cursor.execute('''
                UPDATE tasks
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (new_status, task_id))
            
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error updating task status: {e}")
            return False
        finally:
            if conn:
                conn.close()
    
    def update_task(self, task_id, user_id, **kwargs):
        """Update task details if user has access to the board"""
        if 'status' in kwargs and kwargs['status'] not in self.VALID_STATUSES:
            print(f"Error: Status must be one of {self.VALID_STATUSES}")
            return False
            
        if not kwargs:
            print("Error: No update parameters provided.")
            return False
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # First get task and board details
            cursor.execute('''
                SELECT t.board_id, t.user_id, b.owner_id, b.board_type
                FROM tasks t
                JOIN boards b ON t.board_id = b.id
                WHERE t.id = ?
            ''', (task_id,))
            
            result = cursor.fetchone()
            if not result:
                print(f"Error: Task with ID {task_id} not found.")
                return False
                
            board_id, task_owner_id, board_owner_id, board_type = result
            
            # Check permissions
            is_task_owner = user_id == task_owner_id
            is_board_owner = user_id == board_owner_id
            is_admin_on_public = board_type == 'public' and self.is_admin(user_id)
            
            has_permission = is_task_owner or is_board_owner or is_admin_on_public
            
            if not has_permission:
                print(f"Error: User {user_id} does not have permission to update task {task_id}")
                return False
            
            # Update the task
            set_clause = ', '.join([f"{field} = ?" for field in kwargs.keys()])
            set_clause += ", updated_at = CURRENT_TIMESTAMP"
            
            cursor.execute(f"UPDATE tasks SET {set_clause} WHERE id = ?", 
                list(kwargs.values()) + [task_id])
            
            conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error updating task: {e}")
            return False
    
    def list_all_boards(self, admin_id):
        """List all boards (admin only)"""
        if not self.is_admin(admin_id):
            print(f"Error: User {admin_id} does not have admin privileges to view all boards")
            return []
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id, name, description, board_type, owner_id, created_at, updated_at
                FROM boards
                ORDER BY board_type, owner_id, name
            ''')
            
            boards = cursor.fetchall()
            
            columns = ['id', 'name', 'description', 'board_type', 'owner_id', 'created_at', 'updated_at']
            board_list = [dict(zip(columns, board)) for board in boards]
            
            return board_list
        except sqlite3.Error as e:
            print(f"Error listing all boards: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_user_stats(self, admin_id):
        """Get stats on all users (admin only)"""
        if not self.is_admin(admin_id):
            print(f"Error: User {admin_id} does not have admin privileges to view user stats")
            return None
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get counts of personal boards per user
            cursor.execute('''
                SELECT owner_id, COUNT(*) as personal_board_count
                FROM boards
                WHERE board_type = 'personal'
                GROUP BY owner_id
            ''')
            
            personal_boards = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get counts of public boards per user
            cursor.execute('''
                SELECT owner_id, COUNT(*) as public_board_count
                FROM boards
                WHERE board_type = 'public'
                GROUP BY owner_id
            ''')
            
            public_boards = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get task counts per user
            cursor.execute('''
                SELECT user_id, COUNT(*) as task_count
                FROM tasks
                GROUP BY user_id
            ''')
            
            task_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get task counts by status per user
            cursor.execute('''
                SELECT user_id, status, COUNT(*) as count
                FROM tasks
                GROUP BY user_id, status
            ''')
            
            status_counts = {}
            for row in cursor.fetchall():
                user_id, status, count = row
                if user_id not in status_counts:
                    status_counts[user_id] = {s: 0 for s in self.VALID_STATUSES}
                status_counts[user_id][status] = count
            
            # Combine into user records
            all_users = set(personal_boards.keys()) | set(public_boards.keys()) | set(task_counts.keys())
            
            user_stats = {}
            for user_id in all_users:
                user_stats[user_id] = {
                    'user_id': user_id,
                    'personal_boards': personal_boards.get(user_id, 0),
                    'public_boards': public_boards.get(user_id, 0),
                    'total_tasks': task_counts.get(user_id, 0),
                    'task_status': status_counts.get(user_id, {s: 0 for s in self.VALID_STATUSES})
                }
            
            return user_stats
        except sqlite3.Error as e:
            print(f"Error getting user stats: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def migrate_legacy_data(self):
        """Migrate data from old schema to new schema with boards"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Check if legacy tasks exist but no boards exist
            cursor.execute("SELECT COUNT(*) FROM tasks WHERE board_id IS NULL")
            legacy_task_count = cursor.fetchone()[0]
            
            if legacy_task_count == 0:
                print("No legacy data to migrate.")
                return True
                
            # Create a default personal board for each user
            cursor.execute('''
                SELECT DISTINCT user_id FROM tasks WHERE board_id IS NULL
            ''')
            
            users = [row[0] for row in cursor.fetchall()]
            
            # Create board mapping
            user_board_map = {}
            
            for user_id in users:
                # Create a personal board for this user
                cursor.execute('''
                    INSERT INTO boards (name, owner_id, description, board_type)
                    VALUES (?, ?, ?, ?)
                ''', (f"Personal Board", user_id, "Migrated from legacy data", "personal"))
                
                personal_board_id = cursor.lastrowid
                user_board_map[user_id] = personal_board_id
                
                # Update all legacy tasks for this user
                cursor.execute('''
                    UPDATE tasks 
                    SET board_id = ? 
                    WHERE user_id = ? AND board_id IS NULL
                ''', (personal_board_id, user_id))
            
            conn.commit()
            print(f"Successfully migrated {legacy_task_count} tasks for {len(users)} users.")
            return True
        except sqlite3.Error as e:
            print(f"Error migrating legacy data: {e}")
            return False
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