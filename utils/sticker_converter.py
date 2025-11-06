import gzip
import json
import logging
import os
import tempfile
from typing import Optional, Union, Dict, Any


import ffmpeg
from lottie import objects, parsers
from lottie.exporters import gif
from PIL import Image

# 假设你有这些常量定义
class WxLimitConstants:
    MAX_GIF_SIZE = 1024 * 1024  # 1MB
    IS_ZIP = False

logger = logging.getLogger(__name__)

class ConverterHelper:
    def __init__(self):
        """初始化转换器"""
        # Python 的 ffmpeg-python 不需要设置路径，直接使用系统的 ffmpeg
        pass
    
    def _generate_output_filename(self, input_file: Union[str, bytes], default_name: str = "output") -> str:
        """
        根据输入文件生成默认的 GIF 输出文件名
        
        Args:
            input_file: 输入文件路径或字节数据
            default_name: 当输入为字节数据时的默认文件名
            
        Returns:
            生成的 GIF 文件路径
        """
        if isinstance(input_file, str):
            # 获取文件名（不含路径）并替换后缀为 .gif
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            directory = os.path.dirname(input_file)
            if directory:
                return os.path.join(directory, f"{base_name}.gif")
            else:
                return f"{base_name}.gif"
        else:
            # 字节数据情况下使用默认名称
            return f"{default_name}.gif"
    
    async def webp_to_gif(self, input_file: Union[str, bytes], output_file: Optional[str] = None) -> str:
        """
        将 WebP 转换为 GIF
        
        Args:
            input_file: 输入文件路径或字节数据
            output_file: 输出文件路径
            
        Returns:
            生成的 GIF 文件路径
        """
        try:
            if output_file is None:
                output_file = self._generate_output_filename(input_file, "webp_converted")

            if isinstance(input_file, bytes):
                # 如果是字节数据，先保存为临时文件
                with tempfile.NamedTemporaryFile(suffix='.webp', delete=False) as temp_file:
                    temp_file.write(input_file)
                    temp_input = temp_file.name
            else:
                temp_input = input_file
            
            # 使用 PIL 进行转换
            with Image.open(temp_input) as img:
                # 如果是动画 WebP
                if getattr(img, 'is_animated', False):
                    frames = []
                    durations = []
                    
                    for frame_idx in range(img.n_frames):
                        img.seek(frame_idx)
                        frame = img.copy().convert('RGBA')
                        frames.append(frame)
                        
                        # 获取帧持续时间
                        duration = img.get('info', {}).get('duration', 100)
                        durations.append(duration)
                    
                    # 保存为 GIF
                    if frames:
                        frames[0].save(
                            output_file,
                            'GIF',
                            save_all=True,
                            append_images=frames[1:],
                            duration=durations,
                            loop=0,
                            optimize=True
                        )
                else:
                    # 静态图片直接转换
                    img.convert('RGB').save(output_file, 'GIF')
            
            # 清理临时文件
            if isinstance(input_file, bytes) and os.path.exists(temp_input):
                os.unlink(temp_input)
            
            logger.info(f'WebP to GIF conversion finished! Output: {output_file}')
            return output_file
            
        except Exception as err:
            logger.info(f'Error during WebP to GIF conversion: {err}')
            raise err
    
    async def webm_to_gif(self, input_file: Union[str, bytes], output_file: Optional[str] = None) -> str:
        """
        将 WebM 转换为 GIF
        
        Args:
            input_file: 输入文件路径或字节数据
            output_file: 输出文件路径
            
        Returns:
            生成的 GIF 文件路径
        """
        try:
            if output_file is None:
                output_file = self._generate_output_filename(input_file, "webm_converted")

            if isinstance(input_file, bytes):
                # 如果是字节数据，先保存为临时文件
                with tempfile.NamedTemporaryFile(suffix='.webm', delete=False) as temp_file:
                    temp_file.write(input_file)
                    temp_input = temp_file.name
            else:
                temp_input = input_file
            
            def convert_with_params(resolution: int, fps: int) -> bool:
                """递归转换函数"""
                try:
                    # 构建 scale 参数
                    if resolution < 410:
                        scale_filter = f'scale={resolution}:-1:flags=lanczos'
                    else:
                        scale_filter = 'scale=iw:-1:flags=lanczos'
                    
                    # 使用 ffmpeg-python 进行转换
                    (
                        ffmpeg
                        .input(temp_input)
                        .output(
                            output_file,
                            vf=f'fps={fps},{scale_filter}',
                            f='gif'
                        )
                        .overwrite_output()
                        .run(quiet=True)
                    )
                    
                    logger.info('WebM to GIF conversion finished successfully')
                    
                    # 检查文件大小
                    if os.path.exists(output_file) and WxLimitConstants.IS_ZIP:
                        file_size = os.path.getsize(output_file)
                        
                        if file_size > WxLimitConstants.MAX_GIF_SIZE:
                            logger.info(f'文件大小 {file_size} 超过 1MB，重新调整参数')
                            if resolution > 100 and fps > 1:
                                # 递归调用，降低分辨率和帧率
                                return convert_with_params(resolution - 50, fps - 1)
                            else:
                                raise Exception('无法将文件压缩到 1MB 以下')
                        else:
                            logger.info(f'文件大小 {file_size} 满足要求')
                            return True
                    else:
                        return True
                    
                    return False
                    
                except ffmpeg.Error as e:
                    logger.info(f'FFmpeg error: {e}')
                    raise e
                except Exception as e:
                    logger.info(f'Conversion error: {e}')
                    raise e
            
            # 初始参数
            initial_resolution = 360 + 50  # 410
            initial_fps = 16 + 1  # 17
            
            # 开始转换
            success = convert_with_params(initial_resolution, initial_fps)
            
            # 清理临时文件
            if isinstance(input_file, bytes) and os.path.exists(temp_input):
                os.unlink(temp_input)
            
            if not success:
                raise Exception('WebM to GIF conversion failed')
            
            return output_file
                
        except Exception as err:
            logger.info(f'Error during WebM to GIF conversion: {err}')
            raise err
    
    async def tgs_to_gif(self, input_file: Union[str, bytes], output_file: Optional[str] = None, 
                        lottie_config: Optional[Dict[str, int]] = None) -> str:
        """
        将 TGS (Telegram 贴纸) 转换为 GIF
        
        Args:
            input_file: 输入文件路径或字节数据
            output_file: 输出文件路径
            lottie_config: Lottie 配置参数
            
        Returns:
            生成的 GIF 文件路径
        """
        try:
            if output_file is None:
                output_file = self._generate_output_filename(input_file, "tgs_converted")

            # 默认配置
            default_config = {
                'width': 512,
                'height': 512,
                'fps': 30
            }
            
            if lottie_config:
                default_config.update(lottie_config)

            # 处理输入文件
            if isinstance(input_file, bytes):
                # 如果是字节数据，先保存为临时文件
                with tempfile.NamedTemporaryFile(suffix='.tgs', delete=False) as temp_file:
                    temp_file.write(input_file)
                    temp_input = temp_file.name
            else:
                temp_input = input_file

            # 使用 lottie-python（推荐）
            success = await self._convert_tgs_with_lottie(temp_input, output_file, default_config)
            
            # 清理临时文件
            if isinstance(input_file, bytes) and os.path.exists(temp_input):
                os.unlink(temp_input)

            if not success:
                raise Exception('All TGS conversion methods failed')

            logger.info(f'TGS to GIF conversion finished! Output: {output_file}')
            return output_file
            
        except Exception as err:
            logger.error(f'Error during TGS to GIF conversion: {err}')
            raise err

    async def _convert_tgs_with_lottie(self, input_file: str, output_file: str, config: Dict[str, int]) -> bool:
        """使用 lottie-python 转换"""
        try:
            # 解压 TGS 文件（TGS 是 gzip 压缩的 JSON）
            with gzip.open(input_file, 'rt') as f:
                lottie_data = json.load(f)
            
            # 解析 Lottie 动画
            animation = parsers.tgs.parse_tgs(lottie_data)
            
            # 导出为 GIF
            gif.export_gif(
                animation, 
                output_file,
                width=config['width'],
                height=config['height'],
                fps=config['fps']
            )
            
            logger.info('TGS converted using lottie-python')
            return True
            
        except ImportError:
            logger.warning('lottie-python not installed, trying alternative method')
            return False
        except Exception as e:
            logger.warning(f'lottie-python conversion failed: {e}')
            return False

    def extract_thumbnail(self, video_path: str, output_image: str, time: str = '00:00:01') -> str:
        """
        从视频中提取缩略图
        
        Args:
            video_path: 视频文件路径
            output_image: 输出图片文件名
            time: 提取时间点
            
        Returns:
            输出文件路径
        """
        try:
            (
                ffmpeg
                .input(video_path, ss=time)
                .output(
                    output_image,
                    vframes=1,
                    vf='scale=320:-1'  # 限制宽度，高度自适应
                )
                .overwrite_output()
                .run(quiet=True)
            )
            
            logger.info(f'Thumbnail extracted: {output_image}')
            return output_image
            
        except ffmpeg.Error as e:
            logger.info(f'Error extracting thumbnail: {e}')
            raise e
    
    def get_video_duration(self, video_path: str) -> int:
        """
        获取视频时长
        
        Args:
            video_path: 视频文件路径
            
        Returns:
            视频时长（秒）
        """
        try:
            probe = ffmpeg.probe(video_path)
            duration = float(probe['format']['duration'])
            return int(duration)  # 返回整数秒
            
        except ffmpeg.Error as e:
            logger.info(f'Error getting video duration: {e}')
            raise e
        except KeyError as e:
            logger.info(f'Duration information not found: {e}')
            raise e

converter = ConverterHelper()