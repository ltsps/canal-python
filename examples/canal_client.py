import time
import pymysql
import logging
from typing import Dict, Any, Optional, List

from canal.client import Client
from canal.protocol import EntryProtocol_pb2
from canal.protocol import CanalProtocol_pb2

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============ 目标数据库配置 ============
TARGET_DB_CONFIG = {
    'host': '$target_db_ip',
    'port': 3306,
    'user': '$username',
    'password': '$password',
    'database': '$target_database',
    'charset': 'utf8mb4'
}

# 表名映射（可选）：{源表名：目标表名}
TABLE_MAPPING = {
     '$src_table': '$dts_table'
}

# 是否启用 UPSERT（INSERT 失败时 UPDATE）
ENABLE_UPSERT = True


class TargetDatabaseWriter:
    """目标数据库写入器"""
    
    def __init__(self, db_config: Dict[str, Any], table_mapping: Dict[str, str] = None):
        self.db_config = db_config
        self.table_mapping = table_mapping or {}
        self.connection: Optional[pymysql.Connection] = None
        self._connect()
    
    def _connect(self):
        """建立数据库连接"""
        try:
            self.connection = pymysql.connect(**self.db_config)
            logger.info(f"成功连接到目标数据库：{self.db_config['database']}")
        except Exception as e:
            logger.error(f"连接目标数据库失败：{e}")
            raise
    
    def _get_target_table(self, source_table: str) -> str:
        """获取目标表名（支持表名映射）"""
        return self.table_mapping.get(source_table, source_table)
    
    def _build_insert_sql(self, table: str, data: Dict[str, Any]) -> tuple:
        """构建 INSERT SQL"""
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join([f'`{col}`' for col in columns])
        sql = f"INSERT INTO `{table}` ({column_names}) VALUES ({placeholders})"
        return sql, values
    
    def _build_update_sql(self, table: str, data: Dict[str, Any], primary_keys: List[str]) -> tuple:
        """构建 UPDATE SQL"""
        set_clause = ', '.join([f'`{col}` = %s' for col in data.keys()])
        where_clause = ' AND '.join([f'`{col}` = %s' for col in primary_keys])
        sql = f"UPDATE `{table}` SET {set_clause} WHERE {where_clause}"
        values = list(data.values()) + [data[pk] for pk in primary_keys]
        return sql, values
    
    def _build_delete_sql(self, table: str, data: Dict[str, Any], primary_keys: List[str]) -> tuple:
        """构建 DELETE SQL"""
        where_clause = ' AND '.join([f'`{col}` = %s' for col in primary_keys])
        sql = f"DELETE FROM `{table}` WHERE {where_clause}"
        values = [data[pk] for pk in primary_keys]
        return sql, values
    
    def write(self, database: str, table: str, event_type: Any,
              data: Dict[str, Any], primary_keys: List[str] = None) -> bool:
        """写入数据到目标数据库"""
        if not self.connection:
            self._connect()

        target_table = self._get_target_table(table)
        cursor = None
        success = False
        event_name = 'UNKNOWN'

        try:
            cursor = self.connection.cursor()

            # 兼容整数和枚举类型的 event_type
            if isinstance(event_type, int):
                event_type_int = event_type
                is_insert = event_type_int == 1
                is_update = event_type_int == 2
                is_delete = event_type_int == 3
                event_name = {1: 'INSERT', 2: 'UPDATE', 3: 'DELETE'}.get(event_type_int, 'UNKNOWN')
            else:
                event_type_int = int(event_type)
                is_insert = event_type == EntryProtocol_pb2.EventType.INSERT
                is_update = event_type == EntryProtocol_pb2.EventType.UPDATE
                is_delete = event_type == EntryProtocol_pb2.EventType.DELETE
                event_name = EntryProtocol_pb2.EventType.Name(event_type_int)

            if is_insert:
                sql, values = self._build_insert_sql(target_table, data)
                try:
                    cursor.execute(sql, values)
                    logger.debug(f"INSERT -> {target_table}: {data}")
                except pymysql.err.IntegrityError as e:
                    if e.args[0] == 1062:  # 主键冲突
                        # 转为 UPDATE
                        if not primary_keys:
                            logger.warning(f"INSERT 主键冲突但缺少主键信息：{table}")
                            return False
                        sql, values = self._build_update_sql(target_table, data, primary_keys)
                        cursor.execute(sql, values)
                        logger.debug(f"INSERT 冲突转为 UPDATE -> {target_table}: {data}")
                        event_name = 'UPSERT'
                    else:
                        raise

            elif is_update:
                if not primary_keys:
                    logger.warning(f"UPDATE 操作缺少主键信息，跳过：{table}")
                    return False
                # 先尝试 UPDATE
                sql, values = self._build_update_sql(target_table, data, primary_keys)
                affected = cursor.execute(sql, values)
                if affected > 0:
                    logger.debug(f"UPDATE -> {target_table}: {data}")
                else:
                    # affected == 0 可能是值未变化，尝试 INSERT (UPSERT)
                    try:
                        sql, values = self._build_insert_sql(target_table, data)
                        cursor.execute(sql, values)
                        logger.debug(f"UPSERT -> INSERT {target_table}: {data}")
                        event_name = 'UPSERT'
                    except pymysql.err.IntegrityError:
                        # INSERT 也冲突，说明记录存在但值未变，忽略
                        logger.debug(f"UPDATE 值未变化，跳过：{target_table}")
                        event_name = 'UPDATE (no change)'
                        success = True  # 视为成功

            elif is_delete:
                if not primary_keys:
                    logger.warning(f"DELETE 操作缺少主键信息，跳过：{table}")
                    return False
                sql, values = self._build_delete_sql(target_table, data, primary_keys)
                cursor.execute(sql, values)
                logger.debug(f"DELETE -> {target_table}: {data}")
            else:
                logger.warning(f"未知的事件类型：{event_type}")
                return False

            self.connection.commit()
            logger.info(f"同步成功：{event_name} {database}.{table} -> {target_table}")
            return True

        except Exception as e:
            if self.connection:
                self.connection.rollback()
            logger.error(f"写入失败：{database}.{table} -> {e}")
            return False
        finally:
            if cursor:
                cursor.close()
    
    def close(self):
        """关闭连接"""
        if self.connection:
            self.connection.close()
            logger.info("已关闭目标数据库连接")


def get_primary_keys(table_name: str) -> List[str]:
    """
    获取表的主键字段
    可根据实际需要扩展，这里返回常见的 id 字段
    """
    # 常见主键字段名
    common_pks = ['id', 'uuid', 'code', 'no', 'order_no', 'transaction_id']
    return common_pks[:1]  # 默认返回第一个


# 初始化客户端
client = Client()
client.connect(host='$canal_server_ip', port=$canal_server_port[11111])
client.check_valid(username=b'canal', password=b'')
client.subscribe(client_id=b'1001', destination=b'test', filter=b'.*\\..*')

# 初始化写入器
writer = TargetDatabaseWriter(TARGET_DB_CONFIG, TABLE_MAPPING)


# ============ 重试配置 ============
MAX_RETRIES = -1  # 最大重试次数，-1 表示无限重试
RETRY_DELAY = 5  # 重试间隔（秒）
RETRY_BACKOFF = 1.5  # 退避系数，每次重试延迟乘以该值
HEARTBEAT_INTERVAL = 30  # 心跳间隔（秒），0 表示不发送心跳


def create_client() -> Client:
    """创建并初始化 Canal 客户端"""
    client = Client()
    client.connect(host='$canal_server_ip', port=$canal_server_port[11111])
    client.check_valid(username=b'canal', password=b'')
    client.subscribe(client_id=b'1001', destination=b'$canal_instance', filter=b'.*\\..*')
    logger.info("成功订阅 Canal: destination=test, client_id=1001")
    return client


def reconnect_with_retry(client: Client, retry_count: int, retry_delay: float) -> tuple:
    """带退避的重连"""
    logger.info(f"尝试重连... (第 {retry_count} 次，延迟 {retry_delay:.1f}秒)")
    time.sleep(retry_delay)
    try:
        new_client = create_client()
        return new_client, True
    except Exception as e:
        logger.error(f"重连失败：{e}")
        return client, False


client = create_client()

retry_count = 0
current_delay = RETRY_DELAY
last_heartbeat = time.time()

while True:
    try:
        message = client.get(100)
        entries = message['entries']
        
        # 重置重试计数器
        retry_count = 0
        current_delay = RETRY_DELAY
        
        for entry in entries:
            entry_type = entry.entryType
            if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN, EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
                continue
            row_change = EntryProtocol_pb2.RowChange()
            row_change.MergeFromString(entry.storeValue)
            header = entry.header
            database = header.schemaName
            table = header.tableName
            
            # 使用 header.eventType（整数）：1=INSERT, 2=UPDATE, 3=DELETE
            event_type_int = header.eventType
            
            for row in row_change.rowDatas:
                # 正确构建字段字典
                format_data = dict()
                if event_type_int == 3:  # DELETE
                    for column in row.beforeColumns:
                        format_data[column.name] = column.value
                elif event_type_int == 1:  # INSERT
                    for column in row.afterColumns:
                        format_data[column.name] = column.value
                elif event_type_int == 2:  # UPDATE
                    format_data['before'] = dict()
                    format_data['after'] = dict()
                    for column in row.beforeColumns:
                        format_data['before'][column.name] = column.value
                    for column in row.afterColumns:
                        format_data['after'][column.name] = column.value

                # 写入目标数据库
                primary_keys = get_primary_keys(table)
                # UPDATE/DELETE 使用正确的数据
                if event_type_int == 2:  # UPDATE
                    pk_data = format_data.get('after', {})
                elif event_type_int == 3:  # DELETE
                    pk_data = format_data
                else:  # INSERT
                    pk_data = format_data

                writer.write(database, table, event_type_int, pk_data, primary_keys)

        # 心跳检测（长时间无数据时发送空请求保持连接）
        if HEARTBEAT_INTERVAL > 0 and time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
            logger.debug("发送心跳...")
            last_heartbeat = time.time()
        
        time.sleep(1)
        
    except (ConnectionResetError, BrokenPipeError, OSError) as e:
        retry_count += 1
        
        if MAX_RETRIES > 0 and retry_count > MAX_RETRIES:
            logger.error(f"达到最大重试次数 ({MAX_RETRIES})，退出程序")
            break
        
        logger.error(f"连接断开：{e}")
        
        # 带退避的重连
        client, success = reconnect_with_retry(client, retry_count, current_delay)
        if success:
            logger.info("重连成功，继续同步")
            retry_count = 0
            current_delay = RETRY_DELAY
        else:
            # 重连失败，增加延迟
            current_delay *= RETRY_BACKOFF
            logger.info(f"下次重试延迟：{current_delay:.1f}秒")
            
    except Exception as e:
        logger.error(f"处理消息异常：{e}")
        time.sleep(1)

client.disconnect()
