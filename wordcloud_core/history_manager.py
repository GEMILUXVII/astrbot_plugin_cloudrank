"""
聊天历史记录管理器
"""
import re
import asyncio
from typing import List, Dict, Any, Optional, Tuple
import traceback

from sqlalchemy import Column, Integer, String, Boolean, Index, select, func
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncSession

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent
from astrbot.api.star import Context

from ..utils import get_current_timestamp, get_day_start_end_timestamps, extract_group_id_from_session


class Base(DeclarativeBase):
    pass


class MessageHistory(Base):
    """聊天消息历史记录模型"""
    __tablename__ = 'wordcloud_message_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String, nullable=False)
    sender_id = Column(String, nullable=False)
    sender_name = Column(String)
    message = Column(String, nullable=False)
    timestamp = Column(Integer, nullable=False)
    is_group = Column(Boolean, nullable=False)

    # 索引
    __table_args__ = (
        Index('idx_wordcloud_session_id', 'session_id'),
        Index('idx_wordcloud_timestamp', 'timestamp'),
        Index('idx_wordcloud_session_timestamp', 'session_id', 'timestamp'),
    )


class HistoryManager:
    """聊天历史记录管理器类"""

    def __init__(self, context: Context):
        """
        初始化历史记录管理器

        Args:
            context: AstrBot上下文
        """
        self.context = context
        self.db = self.context.get_db()
        
        # 初始化数据库
        asyncio.create_task(self._ensure_table())

    async def _ensure_table(self) -> None:
        """确保数据库中有消息历史表"""
        try:
            # 使用异步session创建表
            async with self.db.get_db() as session:
                conn = await session.connection()
                await conn.run_sync(Base.metadata.create_all)
            logger.info("WordCloud历史消息表和索引创建成功或已存在")
        except Exception as e:
            logger.error(f"创建WordCloud历史消息表失败: {e}")
            logger.error(traceback.format_exc())



    async def save_message(self, event: AstrMessageEvent) -> bool:
        """
        保存消息到历史记录

        Args:
            event: 消息事件

        Returns:
            是否保存成功
        """
        try:
            # 获取基本信息
            sender_id = event.get_sender_id()
            sender_name = event.get_sender_name()
            message = event.message_str if hasattr(event, "message_str") else None
            timestamp = get_current_timestamp()

            group_id_val = event.get_group_id()
            is_group = bool(group_id_val)

            # 构建会话ID
            session_id_to_save: str
            if group_id_val:  # 群聊消息
                platform_name = event.get_platform_name() or "unknown_platform"
                session_id_to_save = f"{platform_name}_group_{group_id_val}"
            else:  # 私聊消息
                session_id_to_save = event.unified_msg_origin

            # 处理空消息
            if message is None:
                try:
                    # 尝试从消息链中提取文本
                    if hasattr(event, "get_messages") and callable(getattr(event, "get_messages")):
                        messages = event.get_messages()
                        text_parts = []
                        for msg in messages:
                            if hasattr(msg, "text") and msg.text:
                                text_parts.append(msg.text)
                        if text_parts:
                            message = " ".join(text_parts)

                    # 尝试从message_obj获取内容
                    if not message and hasattr(event, "message_obj"):
                        if hasattr(event.message_obj, "raw_message"):
                            message = event.message_obj.raw_message
                        elif hasattr(event.message_obj, "message"):
                            message = str(event.message_obj.message)
                except Exception as e:
                    logger.debug(f"尝试提取消息内容失败: {e}")

                if not message:
                    logger.debug(f"跳过None消息: 会话ID={session_id_to_save}, 发送者={sender_name}")
                    return False

            # 确保message是字符串并清理内容
            try:
                message = str(message)
            except:
                logger.debug(f"消息内容无法转换为字符串: {type(message)}")
                return False

            cleaned_message = await self._clean_message(message, sender_name)
            if not cleaned_message:
                logger.debug(f"跳过空消息: 会话ID={session_id_to_save}, 发送者={sender_name}")
                return True

            # 创建新的消息记录
            new_message = MessageHistory(
                session_id=session_id_to_save,
                sender_id=sender_id,
                sender_name=sender_name,
                message=cleaned_message,
                timestamp=timestamp,
                is_group=is_group
            )

            # 保存到数据库
            async with self.db.get_db() as session:
                session.add(new_message)
                await session.commit()

            logger.debug(f"消息保存成功 - 会话ID: {session_id_to_save}, 时间戳: {timestamp}")
            return True

        except Exception as e:
            logger.error(f"保存消息到历史记录失败: {e}")
            return False

    async def get_history_messages(
        self, session_id: str, days: int = 7, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        获取指定会话的历史消息

        Args:
            session_id: 会话ID
            days: 获取最近几天的消息
            limit: 最大消息数量

        Returns:
            历史消息列表
        """
        try:
            # 计算起始时间戳
            current_time = get_current_timestamp()
            start_time = current_time - (days * 24 * 60 * 60)

            # 创建查询
            query = (
                select(MessageHistory)
                .where(MessageHistory.session_id == session_id)
                .where(MessageHistory.timestamp >= start_time)
                .order_by(MessageHistory.timestamp.desc())
                .limit(limit)
            )

            async with self.db.get_db() as session:
                result = await session.execute(query)
                messages = result.scalars().all()

                # 转换为字典列表
                message_list = [
                    {
                        "session_id": msg.session_id,
                        "sender_id": msg.sender_id,
                        "sender_name": msg.sender_name,
                        "message": msg.message,
                        "timestamp": msg.timestamp,
                        "is_group": msg.is_group,
                    }
                    for msg in messages
                ]

            logger.debug(
                f"获取到{len(message_list)}条历史消息(会话ID: {session_id}, 天数: {days})"
            )
            return message_list

        except Exception as e:
            logger.error(f"获取历史消息失败: {e}")
            return []

    async def get_active_sessions(self, days: int = 7) -> List[str]:
        """
        获取有活动的会话ID列表

        Args:
            days: 最近几天有活动的会话

        Returns:
            会话ID列表
        """
        try:
            # 计算起始时间戳
            current_time = get_current_timestamp()
            start_time = current_time - (days * 24 * 60 * 60)

            # 创建查询
            query = (
                select(func.distinct(MessageHistory.session_id))
                .where(MessageHistory.timestamp >= start_time)
            )

            async with self.db.get_db() as session:
                result = await session.execute(query)
                sessions = result.scalars().all()

            logger.info(f"获取到{len(sessions)}个活跃会话(天数: {days})")
            return sessions

        except Exception as e:
            logger.error(f"获取活跃会话失败: {e}")
            return []

    async def get_message_texts(
        self, session_id: str, days: int = 7, limit: int = 1000
    ) -> List[str]:
        """
        获取指定会话的消息文本列表

        Args:
            session_id: 会话ID
            days: 获取最近几天的消息
            limit: 最大消息数量

        Returns:
            消息文本列表，按时间顺序返回（旧的在前，新的在后）
        """
        try:
            # 计算起始时间戳
            current_time = get_current_timestamp()
            start_time = current_time - (days * 24 * 60 * 60)

            # 创建查询 - 使用正序，使旧的消息在前
            query = (
                select(MessageHistory.message)
                .where(MessageHistory.session_id == session_id)
                .where(MessageHistory.timestamp >= start_time)
                .order_by(MessageHistory.timestamp.asc())
                .limit(limit)
            )

            async with self.db.get_db() as session:
                result = await session.execute(query)
                messages = result.scalars().all()

            # 过滤掉空消息
            messages = [msg for msg in messages if msg and msg.strip()]

            logger.debug(
                f"获取到{len(messages)}条历史消息(会话ID: {session_id}, 天数: {days})"
            )
            total_chars = sum(len(msg) for msg in messages)
            logger.debug(f"消息文本总长度: {total_chars} 字符")

            return messages

        except Exception as e:
            logger.error(f"获取消息文本失败: {e}")
            return []

    async def get_todays_message_texts(self, session_id: str, limit: int = 1000) -> List[str]:
        """
        获取今天的消息文本列表

        Args:
            session_id: 会话ID
            limit: 最大消息数量限制

        Returns:
            今天的消息文本列表
        """
        try:
            # 获取今天的开始和结束时间戳
            start_timestamp, end_timestamp = get_day_start_end_timestamps()
            logger.info(
                f"获取今日消息 - 会话ID: {session_id}, 时间范围: {start_timestamp} 到 {end_timestamp}"
            )

            # 创建查询
            query = (
                select(MessageHistory.message)
                .where(MessageHistory.session_id == session_id)
                .where(MessageHistory.timestamp >= start_timestamp)
                .where(MessageHistory.timestamp <= end_timestamp)
                .order_by(MessageHistory.timestamp.asc())
                .limit(limit)
            )

            async with self.db.get_db() as session:
                result = await session.execute(query)
                messages = result.scalars().all()

            # 过滤掉空消息
            messages = [msg for msg in messages if msg and isinstance(msg, str) and msg.strip()]

            logger.info(
                f"今日消息获取成功 - 会话ID: {session_id}, 消息数量: {len(messages)}"
            )
            return messages

        except Exception as e:
            logger.error(f"获取今日消息文本失败: {e}")
            return []

    async def get_active_group_sessions(self, days: int = 1) -> List[str]:
        """
        获取有活动的群聊会话ID列表

        Args:
            days: 最近几天有活动的群聊

        Returns:
            群聊会话ID列表
        """
        try:
            # 计算起始时间戳
            current_time = get_current_timestamp()
            start_time = current_time - (days * 24 * 60 * 60)

            # 构建查询，只获取群聊会话
            query = (
                select(func.distinct(MessageHistory.session_id))
                .where(
                    MessageHistory.timestamp >= start_time,
                    MessageHistory.is_group == True
                )
            )

            try:
                async with self.db.get_db() as session:
                    result = await session.execute(query)
                    sessions = result.scalars().all()

                logger.info(f"获取到{len(sessions)}个活跃群聊会话(天数: {days})")
                return sessions
            except Exception as db_error:
                logger.error(f"获取活跃群聊会话数据库操作失败: {db_error}")
                return []

        except Exception as e:
            logger.error(f"获取活跃群聊会话失败: {e}")
            return []

    async def get_message_count_today(self, session_id: str) -> int:
        """
        获取今天的消息数量

        Args:
            session_id: 会话ID

        Returns:
            消息数量
        """
        try:
            # 获取今天的开始和结束时间戳
            start_timestamp, end_timestamp = get_day_start_end_timestamps()

            # 构建查询
            query = (
                select(func.count().label('count'))
                .select_from(MessageHistory)
                .where(
                    MessageHistory.session_id == session_id,
                    MessageHistory.timestamp >= start_timestamp,
                    MessageHistory.timestamp <= end_timestamp
                )
            )

            # 执行查询
            async with self.db.get_db() as session:
                result = await session.execute(query)
                count = result.scalar()

            return count or 0
        except Exception as e:
            logger.error(f"获取今天的消息数量失败: {e}")
            return 0

    async def get_message_count_for_days(self, session_id: str, days: int) -> int:
        """
        获取指定会话在过去N天内的总消息数量。

        Args:
            session_id: 会话ID
            days: 获取最近几天的消息

        Returns:
            指定天数内的消息总数量
        """
        try:
            # 计算起始时间戳
            current_time = get_current_timestamp()
            start_time = current_time - (days * 24 * 60 * 60)

            # 创建查询
            query = (
                select(func.count())
                .where(MessageHistory.session_id == session_id)
                .where(MessageHistory.timestamp >= start_time)
            )

            async with self.db.get_db() as session:
                result = await session.execute(query)
                count = result.scalar()
                if count:
                    logger.debug(
                        f"获取到 {days} 天内消息总数: {count} (会话ID: {session_id})"
                    )
                    return count
                return 0

        except Exception as e:
            logger.error(f"获取 {days} 天内消息总数失败: {e}, session_id={session_id}")
            return 0

    async def get_active_users(
        self, session_id: str, days: int = 1, limit: int = 10
    ) -> List[Tuple[str, str, int]]:
        """
        获取指定会话中最活跃的用户（按发言数量排序）

        Args:
            session_id: 会话ID
            days: 统计最近几天的数据，默认为1天（今天）
            limit: 返回的用户数量限制

        Returns:
            用户活跃度排名列表，格式为 [(user_id, user_name, message_count), ...]
        """
        try:
            # 计算时间范围
            if days == 1:
                # 使用当天时间范围
                start_timestamp, end_timestamp = get_day_start_end_timestamps()
            else:
                # 计算过去days天的时间范围
                current_time = get_current_timestamp()
                start_timestamp = current_time - (days * 24 * 60 * 60)
                end_timestamp = current_time

            # 创建查询
            query = (
                select(
                    MessageHistory.sender_id,
                    MessageHistory.sender_name,
                    func.count().label('message_count')
                )
                .where(MessageHistory.session_id == session_id)
                .where(MessageHistory.timestamp >= start_timestamp)
                .where(MessageHistory.timestamp <= end_timestamp)
                .group_by(MessageHistory.sender_id, MessageHistory.sender_name)
                .order_by(func.count().desc())
                .limit(limit)
            )

            async with self.db.get_db() as session:
                result = await session.execute(query)
                rows = result.all()

                # 转换为所需格式
                user_list = [
                    (
                        row.sender_id,
                        row.sender_name or row.sender_id,  # 如果没有名称，使用ID
                        row.message_count
                    )
                    for row in rows
                ]

                return user_list
        except Exception as e:
            logger.error(f"获取活跃用户失败: {e}, session_id={session_id}, days={days}")
            return []

    async def get_total_users_today(self, session_id: str) -> int:
        """
        获取今天在指定会话中发言的总用户数

        Args:
            session_id: 会话ID

        Returns:
            用户数量
        """
        try:
            # 获取今天的开始和结束时间戳
            start_timestamp, end_timestamp = get_day_start_end_timestamps()

            # 创建查询
            query = (
                select(func.count(func.distinct(MessageHistory.sender_id)))
                .where(MessageHistory.session_id == session_id)
                .where(MessageHistory.timestamp >= start_timestamp)
                .where(MessageHistory.timestamp <= end_timestamp)
            )

            async with self.db.get_db() as session:
                result = await session.execute(query)
                count = result.scalar()
                return count or 0

        except Exception as e:
            logger.error(f"获取今天的用户数量失败: {e}")
            return 0

    async def get_total_users_for_date_range(
        self, session_id: str, start_timestamp: int, end_timestamp: int
    ) -> int:
        """
        获取指定会话在指定时间戳范围内的总独立用户数。

        Args:
            session_id: 会话ID
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳

        Returns:
            独立用户总数
        """
        try:
            # 创建查询
            query = (
                select(func.count(func.distinct(MessageHistory.sender_id)))
                .where(MessageHistory.session_id == session_id)
                .where(MessageHistory.timestamp >= start_timestamp)
                .where(MessageHistory.timestamp <= end_timestamp)
            )

            async with self.db.get_db() as session:
                result = await session.execute(query)
                count = result.scalar()
                if count:
                    logger.debug(
                        f"会话 {session_id} 在 {start_timestamp}-{end_timestamp} 范围内总用户数: {count}"
                    )
                    return count
                return 0

        except Exception as e:
            logger.error(f"获取指定日期范围总用户数失败 (会话 {session_id}): {e}")
            return 0

    async def get_active_users_for_date_range(
        self, session_id: str, start_timestamp: int, end_timestamp: int, limit: int = 10
    ) -> List[Tuple[str, str, int]]:
        """
        获取指定会话在指定时间戳范围内的活跃用户列表（按消息数量排序）。

        Args:
            session_id: 会话ID
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳
            limit: 返回的用户数量上限

        Returns:
            活跃用户列表，每个元素为 (sender_id, sender_name, message_count)
        """
        try:
            # 创建查询
            query = (
                select(
                    MessageHistory.sender_id,
                    MessageHistory.sender_name,
                    func.count().label('message_count')
                )
                .where(MessageHistory.session_id == session_id)
                .where(MessageHistory.timestamp >= start_timestamp)
                .where(MessageHistory.timestamp <= end_timestamp)
                .group_by(MessageHistory.sender_id, MessageHistory.sender_name)
                .order_by(func.count().desc())
                .limit(limit)
            )

            async with self.db.get_db() as session:
                result = await session.execute(query)
                rows = result.all()

                # 转换为所需格式
                active_users = [
                    (
                        row.sender_id,
                        row.sender_name or row.sender_id,  # 如果没有名称，使用ID
                        row.message_count
                    )
                    for row in rows
                ]

                logger.debug(
                    f"会话 {session_id} 在 {start_timestamp}-{end_timestamp} 范围内获取到 {len(active_users)} 个活跃用户 (上限 {limit})"
                )
                return active_users

        except Exception as e:
            logger.error(f"获取指定日期范围活跃用户失败 (会话 {session_id}): {e}")
            return []

    async def extract_group_id_from_session(self, session_id: str) -> Optional[str]:
        """
        从会话ID提取群号 (异步兼容接口，内部委托至 utils.extract_group_id_from_session 以保持兼容)

        Args:
            session_id: 会话ID

        Returns:
            群号，如果不是群聊则返回None
        """
        return extract_group_id_from_session(session_id)

    async def get_messages_by_timestamp_range(
        self,
        session_id: str,
        start_timestamp: int,
        end_timestamp: int,
        limit: int = 1000,
    ) -> List[str]:
        """
        获取指定时间戳范围内的消息文本列表

        Args:
            session_id: 会话ID
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳
            limit: 最大消息数量限制

        Returns:
            指定时间范围内的消息文本列表
        """
        try:
            logger.info(
                f"获取指定时间范围消息 - 会话ID: {session_id}, 时间范围: {start_timestamp} 到 {end_timestamp}"
            )

            # 创建查询
            query = (
                select(MessageHistory.message)
                .where(MessageHistory.session_id == session_id)
                .where(MessageHistory.timestamp >= start_timestamp)
                .where(MessageHistory.timestamp <= end_timestamp)
                .order_by(MessageHistory.timestamp.asc())
                .limit(limit)
            )

            async with self.db.get_db() as session:
                result = await session.execute(query)
                messages = result.scalars().all()

            # 过滤掉空消息
            messages = [msg for msg in messages if msg and isinstance(msg, str) and msg.strip()]

            logger.info(
                f"指定时间范围消息获取成功 - 会话ID: {session_id}, 消息数量: {len(messages)}"
            )
            return messages

        except Exception as e:
            logger.error(f"获取指定时间范围消息文本失败: {e}")
            return []

    async def _clean_message(self, message: str, sender_name: Optional[str] = None) -> str:
        """
        清理消息内容，移除不需要计入词云的元素

        Args:
            message: 原始消息
            sender_name: 发送者昵称，用于移除群聊中的@某人

        Returns:
            清理后的消息
        """
        # 移除指令和相关关键词
        message_lower = message.strip().lower()
        if (message_lower.startswith(('#', '/')) or 
            message_lower.startswith('wc') or 
            message_lower.startswith('词云') or
            '生成词云' in message_lower or
            '/wordcloud' in message_lower):
            return ""
        
        # 移除@某人的内容，包括可能的空格和换行
        # 匹配 @昵称(QQ号) 或 @昵称
        message = re.sub(r"@\s*\S+\s*\(\d+\)|@\s*\S+", "", message)

        # 移除URL
        message = re.sub(r"https?://[\w./?=&-]+", "", message)

        # 移除其他可能不需要的内容，例如CQ码
        message = re.sub(r"\[CQ:[^\]]+\]", "", message)

        # 移除各种标点符号和特殊字符，只保留文本和基本空格
        message = re.sub(r"[^\u4e00-\u9fa5a-zA-Z0-9]+", " ", message).strip()

        return message

    async def close(self):
        """
        关闭历史管理器，释放资源
        """
        logger.info("关闭历史管理器...")
        try:
            # 清理数据和缓存
            if hasattr(self, "word_data"):
                self.word_data = {}
            if hasattr(self, "cached_word_counts"):
                self.cached_word_counts = {}
            logger.info("历史数据缓存已清理")
            logger.info("历史管理器已成功关闭")
        except Exception as e:
            logger.error(f"关闭历史管理器时出错: {e}")
            logger.error(traceback.format_exc())
