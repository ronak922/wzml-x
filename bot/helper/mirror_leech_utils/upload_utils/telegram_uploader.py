from asyncio import sleep, Semaphore, create_task, gather
from logging import getLogger
from os import path as ospath, walk
from re import match as re_match, sub as re_sub
from time import time
from typing import Dict, List, Optional, Tuple

from aioshutil import rmtree
from natsort import natsorted
from PIL import Image
from pyrogram.errors import BadRequest, FloodWait, RPCError

try:
    from pyrogram.errors import FloodPremiumWait
except ImportError:
    FloodPremiumWait = FloodWait
from aiofiles.os import (
    path as aiopath,
    remove,
    rename,
)
from pyrogram.types import (
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
)
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ....core.config_manager import Config
from ....core.tg_client import TgClient
from ...ext_utils.bot_utils import sync_to_async
from ...ext_utils.files_utils import get_base_name, is_archive
from ...ext_utils.status_utils import get_readable_file_size, get_readable_time
from ...ext_utils.media_utils import (
    get_audio_thumbnail,
    get_document_type,
    get_media_info,
    get_multiple_frames_thumbnail,
    get_video_thumbnail,
    get_md5_hash,
)
from ...telegram_helper.message_utils import delete_message

LOGGER = getLogger(__name__)

# Add these new configuration options for upload optimization
TELEGRAM_UPLOAD_THREADS = 3  # Number of concurrent uploads
TELEGRAM_UPLOAD_BATCH_SIZE = 5  # Files per batch
TELEGRAM_PROGRESS_UPDATE_INTERVAL = 1048576  # 1MB progress updates
TELEGRAM_THUMBNAIL_CACHE_SIZE = 100  # Max cached thumbnails
CLEAN_LOG_MSG = True  # Clean log messages after upload starts



class TelegramUploader:
    def __init__(self, listener, path):
        self._last_uploaded = 0
        self._processed_bytes = 0
        self._listener = listener
        self._path = path
        self._client = None
        self._start_time = time()
        self._total_files = 0
        self._total_size = 0
        self._thumb = self._listener.thumb or f"thumbnails/{listener.user_id}.jpg"
        self._msgs_dict = {}
        self._corrupted = 0
        self._is_corrupted = False
        self._media_dict = {"videos": {}, "documents": {}}
        self._last_msg_in_group = False
        self._up_path = ""
        self._lprefix = ""
        self._lsuffix = ""
        self._lcaption = ""
        self._lfont = ""
        self._bot_pm = False
        self._media_group = False
        self._is_private = False
        self._sent_msg = None
        self._log_msg = None
        self._user_session = self._listener.user_transmission
        self._error = ""
        
        # Optimization additions
        self._upload_semaphore = Semaphore(Config.TELEGRAM_UPLOAD_THREADS or 3)
        self._thumbnail_cache: Dict[str, Optional[str]] = {}
        self._last_progress_update = 0
        self._progress_update_interval = 1048576  # 1MB
        self._file_queue: List[Tuple[str, str, str]] = []

    async def _upload_progress(self, current, total):
        if self._listener.is_cancelled:
            if self._user_session:
                TgClient.user.stop_transmission()
            else:
                self._listener.client.stop_transmission()
        
        # Throttle progress updates to reduce overhead
        chunk_size = current - self._last_uploaded
        if chunk_size >= self._progress_update_interval or current == total:
            self._last_uploaded = current
            self._processed_bytes += chunk_size
            self._last_progress_update = time()

    async def _user_settings(self):
        settings_map = {
            "MEDIA_GROUP": ("_media_group", False),
            "BOT_PM": ("_bot_pm", False),
            "LEECH_PREFIX": ("_lprefix", ""),
            "LEECH_SUFFIX": ("_lsuffix", ""),
            "LEECH_CAPTION": ("_lcaption", ""),
            "LEECH_FONT": ("_lfont", ""),
        }

        for key, (attr, default) in settings_map.items():
            setattr(
                self,
                attr,
                self._listener.user_dict.get(key) or getattr(Config, key, default),
            )

        if self._thumb != "none" and not await aiopath.exists(self._thumb):
            self._thumb = None

    async def _msg_to_reply(self):
        if self._listener.up_dest:
            msg_link = (
                self._listener.message.link if self._listener.is_super_chat else ""
            )
            msg = f"""➲ <b><u>Leech Started :</u></b>
┃
┊ <b>User :</b> {self._listener.user.mention} ( #ID{self._listener.user_id} ){f"\n┊ <b>Message Link :</b> <a href='{msg_link}'>Click Here</a>" if msg_link else ""}
╰ <b>Source :</b> <a href='{self._listener.source_url}'>Click Here</a>"""
            try:
                self._log_msg = await TgClient.bot.send_message(
                    chat_id=self._listener.up_dest,
                    text=msg,
                    disable_web_page_preview=True,
                    message_thread_id=self._listener.chat_thread_id,
                    disable_notification=True,
                )
                self._sent_msg = self._log_msg
                if self._user_session:
                    self._sent_msg = await TgClient.user.get_messages(
                        chat_id=self._sent_msg.chat.id,
                        message_ids=self._sent_msg.id,
                    )
                else:
                    self._is_private = self._sent_msg.chat.type.name == "PRIVATE"
            except Exception as e:
                await self._listener.on_upload_error(str(e))
                return False

        elif self._user_session:
            self._sent_msg = await TgClient.user.get_messages(
                chat_id=self._listener.message.chat.id, message_ids=self._listener.mid
            )
            if self._sent_msg is None:
                self._sent_msg = await TgClient.user.send_message(
                    chat_id=self._listener.message.chat.id,
                    text="Deleted Cmd Message! Don't delete the cmd message again!",
                    disable_web_page_preview=True,
                    disable_notification=True,
                )
        else:
            self._sent_msg = self._listener.message
        return True

    async def _get_cached_thumbnail(self, file_path: str, media_type: str) -> Optional[str]:
        """Get cached thumbnail or generate new one"""
        cache_key = f"{file_path}_{media_type}_{ospath.getmtime(file_path)}"
        
        if cache_key in self._thumbnail_cache:
            return self._thumbnail_cache[cache_key]
        
        thumb = None
        try:
            if media_type == "video":
                duration = (await get_media_info(file_path))[0]
                if self._listener.thumbnail_layout:
                    thumb = await get_multiple_frames_thumbnail(
                        file_path,
                        self._listener.thumbnail_layout,
                        self._listener.screen_shots,
                    )
                if thumb is None:
                    thumb = await get_video_thumbnail(file_path, duration)
            elif media_type == "audio":
                thumb = await get_audio_thumbnail(file_path)
            elif media_type == "document":
                thumb = await get_video_thumbnail(file_path, None)
        except Exception as e:
            LOGGER.warning(f"Thumbnail generation failed for {file_path}: {e}")
            thumb = None
        
        self._thumbnail_cache[cache_key] = thumb
        return thumb

    async def _prepare_file(self, pre_file_: str, dirpath: str) -> str:
        """Prepare file with optimized processing"""
        cap_file_ = file_ = pre_file_

        if self._lprefix:
            cap_file_ = self._lprefix.replace(r"\s", " ") + file_
            self._lprefix = re_sub(r"<.*?>", "", self._lprefix).replace(r"\s", " ")
            if not file_.startswith(self._lprefix):
                file_ = f"{self._lprefix}{file_}"

        if self._lsuffix:
            name, ext = ospath.splitext(cap_file_)
            cap_file_ = name + self._lsuffix.replace(r"\s", " ") + ext
            self._lsuffix = re_sub(r"<.*?>", "", self._lsuffix).replace(r"\s", " ")

        cap_mono = (
            f"<{Config.LEECH_FONT}>{cap_file_}</{Config.LEECH_FONT}>"
            if Config.LEECH_FONT
            else cap_file_
        )
        
        if self._lcaption:
            cap_mono = await self._process_caption(cap_file_, dirpath, pre_file_)

        # Optimize filename length
        if len(file_) > 56:
            file_ = self._truncate_filename(file_)

        if pre_file_ != file_:
            new_path = ospath.join(dirpath, file_)
            await rename(self._up_path, new_path)
            self._up_path = new_path

        return cap_mono

    async def _process_caption(self, cap_file_: str, dirpath: str, pre_file_: str) -> str:
        """Process caption with caching for media info"""
        self._lcaption = re_sub(
            r"(\\\||\\\{|\\\}|\\s)",
            lambda m: {r"\|": "%%", r"\{": "&%&", r"\}": "$%$", r"\s": " "}[
                m.group(0)
            ],
            self._lcaption,
        )

        parts = self._lcaption.split("|")
        parts[0] = re_sub(
            r"\{([^}]+)\}", lambda m: f"{{{m.group(1).lower()}}}", parts[0]
        )
        
        up_path = ospath.join(dirpath, pre_file_)
        
        # Cache media info to avoid repeated processing
        cache_key = f"media_info_{up_path}_{ospath.getmtime(up_path)}"
        if cache_key not in self._thumbnail_cache:
            dur, qual, lang, subs = await get_media_info(up_path, True)
            self._thumbnail_cache[cache_key] = (dur, qual, lang, subs)
        else:
            dur, qual, lang, subs = self._thumbnail_cache[cache_key]
        
        cap_mono = parts[0].format(
            filename=cap_file_,
            size=get_readable_file_size(await aiopath.getsize(up_path)),
            duration=get_readable_time(dur),
            quality=qual,
            languages=lang,
            subtitles=subs,
            md5_hash=await sync_to_async(get_md5_hash, up_path),
            mime_type=self._listener.file_details.get("mime_type", "text/plain"),
            prefilename=self._listener.file_details.get("filename", ""),
            precaption=self._listener.file_details.get("caption", ""),
        )

        for part in parts[1:]:
            args = part.split(":")
            cap_mono = cap_mono.replace(
                args[0],
                args[1] if len(args) > 1 else "",
                int(args[2]) if len(args) == 3 else -1,
            )
        
        return re_sub(
            r"%%|&%&|\$%\$",
            lambda m: {"%%": "|", "&%&": "{", "$%$": "}"}[m.group()],
            cap_mono,
        )

    def _truncate_filename(self, file_: str) -> str:
        """Optimize filename truncation"""
        if is_archive(file_):
            name = get_base_name(file_)
            ext = file_.split(name, 1)[1]
        elif match := re_match(r".+(?=\..+\.0*\d+$)|.+(?=\.part\d+\..+$)", file_):
            name = match.group(0)
            ext = file_.split(name, 1)[1]
        elif len(fsplit := ospath.splitext(file_)) > 1:
            name = fsplit[0]
            ext = fsplit[1]
        else:
            name = file_
            ext = ""
        
        if self._lsuffix:
            ext = f"{self._lsuffix}{ext}"
        
        name = name[: 56 - len(ext)]
        return f"{name}{ext}"

    def _get_input_media(self, subkey: str, key: str) -> List:
        """Optimized input media generation"""
        rlist = []
        for msg in self._media_dict[key][subkey]:
            if key == "videos":
                input_media = InputMediaVideo(
                    media=msg.video.file_id, caption=msg.caption
                )
            else:
                input_media = InputMediaDocument(
                    media=msg.document.file_id, caption=msg.caption
                )
            rlist.append(input_media)
        return rlist

    async def _send_screenshots(self, dirpath: str, outputs: List[str]):
        """Optimized screenshot sending with batching"""
        inputs = [
            InputMediaPhoto(ospath.join(dirpath, p), p.rsplit("/", 1)[-1])
            for p in outputs
        ]
        
        # Process in batches of 10 (Telegram limit)
        for i in range(0, len(inputs), 10):
            batch = inputs[i : i + 10]
            try:
                if Config.BOT_PM:
                    await TgClient.bot.send_media_group(
                        chat_id=self._listener.user_id,
                        media=batch,
                        disable_notification=True,
                    )
                self._sent_msg = (
                    await self._sent_msg.reply_media_group(
                        media=batch,
                        quote=True,
                        disable_notification=True,
                    )
                )[-1]
            except Exception as e:
                LOGGER.error(f"Failed to send screenshot batch: {e}")

    async def _send_media_group(self, subkey: str, key: str, msgs: List):
        """Optimized media group sending"""
        try:
            for index, msg in enumerate(msgs):
                if self._listener.hybrid_leech or not self._user_session:
                    msgs[index] = await self._listener.client.get_messages(
                        chat_id=msg[0], message_ids=msg[1]
                    )
                else:
                    msgs[index] = await TgClient.user.get_messages(
                        chat_id=msg[0], message_ids=msg[1]
                    )
            
            msgs_list = await msgs[0].reply_to_message.reply_media_group(
                media=self._get_input_media(subkey, key),
                quote=True,
                disable_notification=True,
            )
            
            # Clean up individual messages
            for msg in msgs:
                if msg.link in self._msgs_dict:
                    del self._msgs_dict[msg.link]
                await delete_message(msg)
            
            del self._media_dict[key][subkey]
            
            if self._listener.is_super_chat or self._listener.up_dest:
                for m in msgs_list:
                    self._msgs_dict[m.link] = m.caption
            
            self._sent_msg = msgs_list[-1]
        except Exception as e:
            LOGGER.error(f"Failed to send media group: {e}")

    async def _copy_media(self):
        """Optimized media copying with error handling"""
        if not self._bot_pm:
            return
        
        try:
            await TgClient.bot.copy_message(
                chat_id=self._listener.user_id,
                from_chat_id=self._sent_msg.chat.id,
                message_id=self._sent_msg.id,
                reply_to_message_id=(
                    self._listener.pm_msg.id if self._listener.pm_msg else None
                ),
            )
        except Exception as err:
            if not self._listener.is_cancelled:
                LOGGER.error(f"Failed To Send in BotPM: {err}")

    async def _collect_files(self) -> List[Tuple[str, str, str]]:
        """Collect all files for upload with size calculation"""
        file_list = []
        total_size = 0
        
        for dirpath, _, files in natsorted(await sync_to_async(walk, self._path)):
            if dirpath.strip().endswith(("/yt-dlp-thumb", "_mltbss")):
                continue
            
            for file_ in natsorted(files):
                f_path = ospath.join(dirpath, file_)
                if await aiopath.exists(f_path):
                    f_size = await aiopath.getsize(f_path)
                    if f_size > 0:
                        file_list.append((dirpath, file_, f_path))
                        total_size += f_size
                    else:
                        LOGGER.warning(f"Skipping zero-size file: {f_path}")
                        self._corrupted += 1
        
        self._total_size = total_size
        return file_list

    async def _upload_file_worker(self, file_info: Tuple[str, str, str]) -> bool:
        """Worker function for concurrent file uploads"""
        dirpath, file_, f_path = file_info
        
        async with self._upload_semaphore:
            try:
                if self._listener.is_cancelled:
                    return False
                
                self._up_path = f_path
                f_size = await aiopath.getsize(f_path)
                
                # Optimize session selection based on file size
                if self._listener.hybrid_leech and self._listener.user_transmission:
                    use_user_session = f_size > 1073741824  # 1GB threshold
                    if use_user_session != self._user_session:
                        self._user_session = use_user_session
                        if self._user_session:
                            self._sent_msg = await TgClient.user.get_messages(
                                chat_id=self._sent_msg.chat.id,
                                message_ids=self._sent_msg.id,
                            )
                        else:
                            self._sent_msg = await self._listener.client.get_messages(
                                chat_id=self._sent_msg.chat.id,
                                message_ids=self._sent_msg.id,
                            )
                
                cap_mono = await self._prepare_file(file_, dirpath)
                self._last_uploaded = 0
                
                await self._upload_file(cap_mono, file_, f_path)
                
                if (
                    not self._is_corrupted
                    and (self._listener.is_super_chat or self._listener.up_dest)
                    and not self._is_private
                ):
                    self._msgs_dict[self._sent_msg.link] = file_
                
                # Reduced sleep for better performance
                await sleep(0.1)
                return True
                
            except Exception as err:
                if isinstance(err, RetryError):
                    LOGGER.info(f"Total Attempts: {err.last_attempt.attempt_number}")
                    err = err.last_attempt.exception()
                
                LOGGER.error(f"{err}. Path: {f_path}", exc_info=True)
                self._error = str(err)
                self._corrupted += 1
                return False
            
            finally:
                # Clean up uploaded file
                if not self._listener.is_cancelled and await aiopath.exists(f_path):
                    await remove(f_path)

    async def upload(self):
        """Main upload function with optimizations"""
        await self._user_settings()
        
        if not await self._msg_to_reply():
            return
        
        is_log_del = False
        
        # Handle special directories first
        for dirpath, _, files in natsorted(await sync_to_async(walk, self._path)):
            if dirpath.strip().endswith("/yt-dlp-thumb"):
                continue
            
            if dirpath.strip().endswith("_mltbss"):
                await self._send_screenshots(dirpath, files)
                await rmtree(dirpath, ignore_errors=True)
                continue
        
        # Collect all files for upload
        file_list = await self._collect_files()
        
        if not file_list:
            await self._listener.on_upload_error(
                "No files to upload. In case you have filled EXCLUDED_EXTENSIONS, then check if all files have those extensions or not."
            )
            return
        
        self._total_files = len(file_list)
        LOGGER.info(f"Starting upload of {self._total_files} files with total size: {get_readable_file_size(self._total_size)}")
        
        # Process files in batches for better memory management
        batch_size = Config.TELEGRAM_UPLOAD_BATCH_SIZE or 5
        successful_uploads = 0
        
        for i in range(0, len(file_list), batch_size):
            if self._listener.is_cancelled:
                return
            
            batch = file_list[i:i + batch_size]
            
            # Create upload tasks for current batch
            upload_tasks = [
                create_task(self._upload_file_worker(file_info))
                for file_info in batch
            ]
            
            # Wait for batch completion
            results = await gather(*upload_tasks, return_exceptions=True)
            
            # Count successful uploads
            for result in results:
                if result is True:
                    successful_uploads += 1
                elif isinstance(result, Exception):
                    LOGGER.error(f"Upload task failed: {result}")
            
            # Delete log message after first successful batch
            if not is_log_del and successful_uploads > 0 and self._log_msg and Config.CLEAN_LOG_MSG:
                await delete_message(self._log_msg)
                is_log_del = True
            
            # Small delay between batches to prevent overwhelming
            if i + batch_size < len(file_list):
                await sleep(0.2)
        
        # Handle remaining media groups
        await self._process_remaining_media_groups()
        
        if self._listener.is_cancelled:
            return
        
        # Final validation
        if successful_uploads == 0:
            await self._listener.on_upload_error(
                f"No files uploaded successfully. {self._error or 'Check logs!'}"
            )
            return
        
        if successful_uploads <= self._corrupted:
            await self._listener.on_upload_error(
                f"Most files corrupted or unable to upload. Successful: {successful_uploads}, Corrupted: {self._corrupted}. {self._error or 'Check logs!'}"
            )
            return
        
        LOGGER.info(f"Leech Completed: {self._listener.name} - {successful_uploads}/{self._total_files} files uploaded")
        await self._listener.on_upload_complete(
            None, self._msgs_dict, successful_uploads, self._corrupted
        )

    async def _process_remaining_media_groups(self):
        """Process any remaining media groups"""
        for key, value in list(self._media_dict.items()):
            for subkey, msgs in list(value.items()):
                if len(msgs) > 1:
                    try:
                        await self._send_media_group(subkey, key, msgs)
                    except Exception as e:
                        LOGGER.error(f"Failed to send remaining media group: {e}")

    @retry(
        wait=wait_exponential(multiplier=1.5, min=2, max=6),
        stop=stop_after_attempt(2),
        retry=retry_if_exception_type((FloodWait, FloodPremiumWait, RPCError)),
    )
    async def _upload_file(self, cap_mono: str, file: str, o_path: str, force_document: bool = False):
        """Optimized file upload with better error handling"""
        if (
            self._thumb is not None
            and not await aiopath.exists(self._thumb)
            and self._thumb != "none"
        ):
            self._thumb = None
        
        thumb = self._thumb
        self._is_corrupted = False
        
        try:
            is_video, is_audio, is_image = await get_document_type(self._up_path)
            
            # Optimized thumbnail handling
            if not is_image and thumb is None:
                file_name = ospath.splitext(file)[0]
                thumb_path = f"{self._path}/yt-dlp-thumb/{file_name}.jpg"
                
                if await aiopath.isfile(thumb_path):
                    thumb = thumb_path
                elif is_audio and not is_video:
                    thumb = await self._get_cached_thumbnail(self._up_path, "audio")
                elif is_video:
                    thumb = await self._get_cached_thumbnail(self._up_path, "video")
            
            if self._listener.is_cancelled:
                return
            
            # Upload based on media type
            if (
                self._listener.as_doc
                or force_document
                or (not is_video and not is_audio and not is_image)
            ):
                await self._upload_as_document(cap_mono, is_video, thumb)
            elif is_video:
                await self._upload_as_video(cap_mono, thumb)
            elif is_audio:
                await self._upload_as_audio(cap_mono, thumb)
            else:
                await self._upload_as_photo(cap_mono)
            
            # Handle media groups
            await self._handle_media_group(o_path)
            
            # Copy to bot PM if needed
            if self._sent_msg:
                await self._copy_media()
            
            # Clean up temporary thumbnail
            if (
                self._thumb is None
                and thumb is not None
                and thumb != "none"
                and await aiopath.exists(thumb)
                and not thumb.startswith(f"{self._path}/yt-dlp-thumb/")
            ):
                await remove(thumb)
                
        except (FloodWait, FloodPremiumWait) as f:
            LOGGER.warning(f"Rate limited: {f}")
            await sleep(f.value * 1.1)  # Reduced multiplier
            
            # Clean up thumbnail
            if (
                self._thumb is None
                and thumb is not None
                and thumb != "none"
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            
            return await self._upload_file(cap_mono, file, o_path, force_document)
            
        except Exception as err:
            # Clean up thumbnail
            if (
                self._thumb is None
                and thumb is not None
                and thumb != "none"
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            
            err_type = "RPCError: " if isinstance(err, RPCError) else ""
            LOGGER.error(f"{err_type}{err}. Path: {self._up_path}", exc_info=True)
            
            # Retry as document if other formats fail
            if isinstance(err, BadRequest) and not force_document:
                LOGGER.info(f"Retrying as document: {self._up_path}")
                return await self._upload_file(cap_mono, file, o_path, True)
            
            raise err

    async def _upload_as_document(self, cap_mono: str, is_video: bool, thumb: Optional[str]):
        """Upload file as document"""
        if is_video and thumb is None:
            thumb = await self._get_cached_thumbnail(self._up_path, "document")
        
        if thumb == "none":
            thumb = None
        
        self._sent_msg = await self._sent_msg.reply_document(
            document=self._up_path,
            quote=True,
            thumb=thumb,
            caption=cap_mono,
            force_document=True,
            disable_notification=True,
            progress=self._upload_progress,
        )

    async def _upload_as_video(self, cap_mono: str, thumb: Optional[str]):
        """Upload file as video"""
        duration = (await get_media_info(self._up_path))[0]
        
        if thumb is None:
            thumb = await self._get_cached_thumbnail(self._up_path, "video")
        
        if thumb is not None and thumb != "none":
            try:
                with Image.open(thumb) as img:
                    width, height = img.size
            except Exception:
                width, height = 480, 320
        else:
            width, height = 480, 320
        
        if self._listener.is_cancelled:
            return
        
        if thumb == "none":
            thumb = None
        
        self._sent_msg = await self._sent_msg.reply_video(
            video=self._up_path,
            quote=True,
            caption=cap_mono,
            duration=duration,
            width=width,
            height=height,
            thumb=thumb,
            supports_streaming=True,
            disable_notification=True,
            progress=self._upload_progress,
        )

    async def _upload_as_audio(self, cap_mono: str, thumb: Optional[str]):
        """Upload file as audio"""
        duration, artist, title = await get_media_info(self._up_path)
        
        if self._listener.is_cancelled:
            return
        
        if thumb == "none":
            thumb = None
        
        self._sent_msg = await self._sent_msg.reply_audio(
            audio=self._up_path,
            quote=True,
            caption=cap_mono,
            duration=duration,
            performer=artist,
            title=title,
            thumb=thumb,
            disable_notification=True,
            progress=self._upload_progress,
        )

    async def _upload_as_photo(self, cap_mono: str):
        """Upload file as photo"""
        if self._listener.is_cancelled:
            return
        
        self._sent_msg = await self._sent_msg.reply_photo(
            photo=self._up_path,
            quote=True,
            caption=cap_mono,
            disable_notification=True,
            progress=self._upload_progress,
        )

    async def _handle_media_group(self, o_path: str):
        """Handle media group processing"""
        if (
            not self._listener.is_cancelled
            and self._media_group
            and (self._sent_msg.video or self._sent_msg.document)
        ):
            key = "documents" if self._sent_msg.document else "videos"
            
            if match := re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", o_path):
                pname = match.group(0)
                
                if pname in self._media_dict[key]:
                    self._media_dict[key][pname].append(
                        [self._sent_msg.chat.id, self._sent_msg.id]
                    )
                else:
                    self._media_dict[key][pname] = [
                        [self._sent_msg.chat.id, self._sent_msg.id]
                    ]
                
                msgs = self._media_dict[key][pname]
                
                # Send media group when we have 10 files (Telegram limit)
                if len(msgs) == 10:
                    await self._send_media_group(pname, key, msgs)
                else:
                    self._last_msg_in_group = True

    @property
    def speed(self) -> float:
        """Calculate upload speed with better precision"""
        try:
            elapsed_time = time() - self._start_time
            if elapsed_time > 0:
                return self._processed_bytes / elapsed_time
            return 0
        except (ZeroDivisionError, AttributeError):
            return 0

    @property
    def processed_bytes(self) -> int:
        """Get processed bytes"""
        return self._processed_bytes

    @property
    def eta(self) -> int:
        """Calculate ETA with better accuracy"""
        try:
            speed = self.speed
            if speed > 0:
                remaining_bytes = self._total_size - self._processed_bytes
                return int(remaining_bytes / speed)
            return 0
        except (ZeroDivisionError, AttributeError):
            return 0

    @property
    def progress(self) -> float:
        """Calculate upload progress percentage"""
        try:
            if self._total_size > 0:
                return (self._processed_bytes / self._total_size) * 100
            return 0
        except (ZeroDivisionError, AttributeError):
            return 0

    async def cancel_task(self):
        """Cancel upload task with cleanup"""
        self._listener.is_cancelled = True
        LOGGER.info(f"Cancelling Upload: {self._listener.name}")
        
        # Clean up thumbnail cache
        self._thumbnail_cache.clear()
        
        # Stop any ongoing transmissions
        try:
            if self._user_session:
                TgClient.user.stop_transmission()
            else:
                self._listener.client.stop_transmission()
        except Exception as e:
            LOGGER.warning(f"Error stopping transmission: {e}")
        
        await self._listener.on_upload_error("Upload cancelled by user!")

    def __del__(self):
        """Cleanup when object is destroyed"""
        try:
            self._thumbnail_cache.clear()
        except Exception:
            pass
