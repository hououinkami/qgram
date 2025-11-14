import asyncio
import gzip
import json
import logging
import os
import tempfile
from io import BytesIO
from typing import Optional, Union, Dict, Any, Tuple

import ffmpeg
from lottie import objects, parsers
from lottie.exporters import gif
from PIL import Image, ImageSequence

# 假设你有这些常量定义
class WxLimitConstants:
    MAX_GIF_SIZE = 1024 * 1024  # 1MB
    IS_ZIP = True

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
        将 WebP 转换为 GIF（使用 FFmpeg，完美保留透明）
        
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
            
            # 🚀 使用 FFmpeg 转换（自动保留透明）
            try:
                (
                    ffmpeg
                    .input(temp_input)
                    .output(
                        output_file,
                        vf='split[s0][s1];[s0]palettegen=reserve_transparent=1[p];[s1][p]paletteuse=alpha_threshold=128',
                        f='gif'
                    )
                    .overwrite_output()
                    .run(quiet=True, capture_stderr=True)
                )
                
                logger.info('WebP to GIF conversion finished (FFmpeg)')
                
            except ffmpeg.Error as e:
                # FFmpeg 失败，回退到 PIL
                logger.warning(f'FFmpeg conversion failed, trying PIL: {e}')
                return await self._webp_to_gif_by_pil(temp_input, output_file)
            
            # 清理临时文件
            if isinstance(input_file, bytes) and os.path.exists(temp_input):
                os.unlink(temp_input)
            
            return output_file
            
        except Exception as err:
            logger.error(f'Error during WebP to GIF conversion: {err}')
            raise err
    
    async def _webp_to_gif_by_pil(self, input_file: str, output_file: str) -> str:
        """PIL 备用方法"""
        with Image.open(input_file) as img:
            if getattr(img, 'is_animated', False):
                frames = []
                durations = []
                
                for frame_idx in range(img.n_frames):
                    img.seek(frame_idx)
                    frame = img.copy().convert('RGBA')
                    frames.append(frame)
                    durations.append(img.info.get('duration', 100))
                
                if frames:
                    frames[0].save(
                        output_file,
                        'GIF',
                        save_all=True,
                        append_images=frames[1:],
                        duration=durations,
                        loop=0,
                        optimize=True,
                        transparency=0,
                        disposal=2
                    )
            else:
                img.convert('RGBA').save(
                    output_file,
                    'GIF',
                    transparency=0,
                    optimize=True
                )
        
        return output_file
    
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
        """使用 lottie-python 转换（智能背景处理）"""
        try:            
            # 解压 TGS 文件
            with gzip.open(input_file, 'rt') as f:
                lottie_data = json.load(f)
            
            # 解析 Lottie 动画
            animation = parsers.tgs.parse_tgs(lottie_data)
            
            # 导出到临时文件
            temp_output = output_file + '.tmp.gif'
            
            # 🆕 尝试使用 bg_color 参数（如果支持）
            try:
                gif.export_gif(
                    animation, 
                    temp_output,
                    width=config['width'],
                    height=config['height'],
                    fps=config['fps'],
                    bg_color=(255, 255, 255, 0)  # 尝试透明背景
                )
            except TypeError:
                # 不支持 bg_color，使用默认
                gif.export_gif(
                    animation, 
                    temp_output,
                    width=config['width'],
                    height=config['height'],
                    fps=config['fps']
                )
            
            # 🆕 后处理：替换黑色背景为透明
            with Image.open(temp_output) as img:
                frames = []
                durations = []
                
                for frame in ImageSequence.Iterator(img):
                    # 转换为 RGBA
                    frame = frame.convert('RGBA')
                    
                    # 获取像素数据
                    pixels = frame.load()
                    width, height = frame.size
                    
                    # 替换黑色为透明（优化版）
                    for y in range(height):
                        for x in range(width):
                            r, g, b, a = pixels[x, y]
                            # 如果是接近黑色的像素（容差 10）
                            if r < 10 and g < 10 and b < 10:
                                pixels[x, y] = (0, 0, 0, 0)  # 设为透明
                    
                    frames.append(frame)
                    
                    # 获取帧持续时间
                    duration = frame.info.get('duration', int(1000 / config['fps']))
                    durations.append(duration)
                
                # 保存为 GIF（透明背景）
                if frames:
                    frames[0].save(
                        output_file,
                        'GIF',
                        save_all=True,
                        append_images=frames[1:],
                        duration=durations,
                        loop=0,
                        optimize=True,
                        transparency=0,
                        disposal=2
                    )
            
            # 删除临时文件
            if os.path.exists(temp_output):
                os.unlink(temp_output)
            
            logger.info('TGS converted with transparent background')
            return True
            
        except ImportError:
            logger.warning('lottie-python not installed')
            return False
        except Exception as e:
            logger.error(f'lottie-python conversion failed: {e}')
            return False

    async def gif_to_webm(self, input_file: Union[str, bytes, BytesIO], output_file: Optional[str] = None) -> str:
        """
        将 GIF 转换为 WebM
        """
        try:
            if output_file is None:
                output_file = self._generate_output_filename(input_file, "gif_converted").replace('.gif', '.webm')

            # 处理不同类型的输入
            if isinstance(input_file, (bytes, BytesIO)):
                with tempfile.NamedTemporaryFile(suffix='.gif', delete=False) as temp_file:
                    if isinstance(input_file, bytes):
                        temp_file.write(input_file)
                    else:  # BytesIO
                        input_file.seek(0)
                        temp_file.write(input_file.read())
                    temp_input = temp_file.name
            else:
                temp_input = input_file

            # 使用独立的GIF分析函数
            # gif_info = await self.analyze_gif(temp_input)

            # 异步运行 FFmpeg 命令
            async def run_ffmpeg_command(cmd, timeout=60):
                try:
                    process = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    
                    try:
                        stdout, stderr = await asyncio.wait_for(
                            process.communicate(), 
                            timeout=timeout
                        )
                        return process.returncode, stdout, stderr
                    except asyncio.TimeoutError:
                        logger.warning(f"FFmpeg command timed out after {timeout}s")
                        process.terminate()
                        try:
                            await asyncio.wait_for(process.wait(), timeout=5)
                        except asyncio.TimeoutError:
                            process.kill()
                            await process.wait()
                        raise TimeoutError(f"FFmpeg command timed out after {timeout} seconds")
                        
                except Exception as e:
                    logger.error(f"Error running FFmpeg command: {e}")
                    raise

            # 转换配置
            telegram_configs = [
                # 🎯 配置1: 双通道编码 - 确保 Duration 正确
                {
                    'name': 'Two-Pass VP9 with Duration Fix',
                    'type': 'two_pass',
                    'pass1_cmd': [
                        'ffmpeg', '-i', temp_input,
                        '-c:v', 'libvpx-vp9',
                        '-pix_fmt', 'yuv420p',
                        '-vf', 'scale=512:512:force_original_aspect_ratio=decrease,pad=512:512:(ow-iw)/2:(oh-ih)/2:color=black,fps=15',
                        '-pass', '1',
                        '-b:v', '200k',
                        '-crf', '30',
                        '-g', '15',
                        '-keyint_min', '5',
                        '-auto-alt-ref', '0',
                        '-lag-in-frames', '0',
                        '-quality', 'good',
                        '-cpu-used', '2',
                        '-threads', '2',
                        '-an',
                        '-f', 'null',
                        '/dev/null'
                    ],
                    'pass2_cmd': [
                        'ffmpeg', '-i', temp_input,
                        '-c:v', 'libvpx-vp9',
                        '-pix_fmt', 'yuv420p',
                        '-vf', 'scale=512:512:force_original_aspect_ratio=decrease,pad=512:512:(ow-iw)/2:(oh-ih)/2:color=black,fps=15',
                        '-pass', '2',
                        '-b:v', '200k',
                        '-crf', '30',
                        '-g', '15',
                        '-keyint_min', '5',
                        '-auto-alt-ref', '0',
                        '-lag-in-frames', '0',
                        '-quality', 'good',
                        '-cpu-used', '2',
                        '-threads', '2',
                        '-an',
                        '-f', 'webm',
                        '-avoid_negative_ts', 'make_zero',
                        '-fflags', '+genpts',
                        '-y', output_file
                    ]
                },
                
                # 🎯 配置2: 强制关键帧 - 确保动画
                {
                    'name': 'Force Keyframes VP9',
                    'type': 'single_pass',
                    'cmd': [
                        'ffmpeg', '-i', temp_input,
                        '-c:v', 'libvpx-vp9',
                        '-pix_fmt', 'yuv420p',
                        '-vf', 'scale=512:512:force_original_aspect_ratio=decrease,pad=512:512:(ow-iw)/2:(oh-ih)/2:color=black,fps=15',
                        '-b:v', '200k',
                        '-minrate', '100k',    # 🔑 最小码率
                        '-maxrate', '300k',    # 🔑 最大码率
                        '-crf', '28',
                        '-g', '15',
                        '-keyint_min', '1',    # 🔑 强制更多关键帧
                        '-force_key_frames', 'expr:gte(t,n_forced*0.5)',  # 🔑 每0.5秒一个关键帧
                        '-auto-alt-ref', '0',
                        '-lag-in-frames', '0',
                        '-quality', 'good',
                        '-cpu-used', '1',      # 🔑 更好的质量
                        '-threads', '4',
                        '-an',
                        '-f', 'webm',
                        '-movflags', '+faststart',
                        '-avoid_negative_ts', 'make_zero',
                        '-fflags', '+genpts',
                        '-y', output_file
                    ]
                },
                
                # 🎯 配置3: 循环输入确保动画
                {
                    'name': 'Loop Input VP9',
                    'type': 'single_pass',
                    'cmd': [
                        'ffmpeg', 
                        '-stream_loop', '1',   # 🔑 循环输入1次
                        '-i', temp_input,
                        '-c:v', 'libvpx-vp9',
                        '-pix_fmt', 'yuv420p',
                        '-vf', 'scale=512:512:force_original_aspect_ratio=decrease,pad=512:512:(ow-iw)/2:(oh-ih)/2:color=black,fps=12',
                        '-b:v', '180k',
                        '-crf', '32',
                        '-g', '12',
                        '-keyint_min', '6',
                        '-auto-alt-ref', '0',
                        '-lag-in-frames', '0',
                        '-quality', 'good',
                        '-cpu-used', '2',
                        '-threads', '2',
                        '-an',
                        '-f', 'webm',
                        '-t', '3.0',           # 🔑 限制总时长
                        '-avoid_negative_ts', 'make_zero',
                        '-fflags', '+genpts',
                        '-y', output_file
                    ]
                },
                
                # 🎯 配置4: 最简单但有效的方法
                {
                    'name': 'Simple Effective VP9',
                    'type': 'single_pass',
                    'cmd': [
                        'ffmpeg', '-i', temp_input,
                        '-c:v', 'libvpx-vp9',
                        '-vf', 'scale=512:512:flags=lanczos,fps=10',  # 🔑 简化滤镜
                        '-b:v', '150k',
                        '-crf', '35',
                        '-g', '10',
                        '-keyint_min', '1',
                        '-auto-alt-ref', '0',
                        '-lag-in-frames', '0',
                        '-quality', 'realtime',  # 🔑 实时质量
                        '-cpu-used', '4',
                        '-threads', '2',
                        '-an',
                        '-f', 'webm',
                        '-t', '2.5',
                        '-avoid_negative_ts', 'make_zero',
                        '-fflags', '+genpts',
                        '-y', output_file
                    ]
                },
                
                # 🎯 配置5: 使用 libwebp 作为后备
                {
                    'name': 'WebP Fallback',
                    'type': 'single_pass',
                    'cmd': [
                        'ffmpeg', '-i', temp_input,
                        '-c:v', 'libwebp',     # 🔑 使用 WebP 编码器
                        '-vf', 'scale=512:512:flags=lanczos,fps=15',
                        '-lossless', '0',
                        '-compression_level', '4',
                        '-quality', '80',
                        '-preset', 'default',
                        '-loop', '0',          # 🔑 无限循环
                        '-an',
                        '-f', 'webm',
                        '-t', '3.0',
                        '-y', output_file
                    ]
                }
            ]

            last_error = None
            
            for i, config in enumerate(telegram_configs):
                try:
                    logger.info(f'🔄 Trying configuration {i+1}/{len(telegram_configs)}: {config["name"]}')
                    
                    # 执行转换
                    if config['type'] == 'two_pass':
                        # 双通道编码
                        returncode1, stdout1, stderr1 = await run_ffmpeg_command(config['pass1_cmd'], timeout=60)
                        if returncode1 != 0:
                            error_msg = stderr1.decode('utf-8', errors='ignore') if stderr1 else 'Unknown error'
                            logger.warning(f'❌ Pass 1 failed: {error_msg[:200]}...')
                            continue
                        
                        returncode, stdout, stderr = await run_ffmpeg_command(config['pass2_cmd'], timeout=60)
                    else:
                        # 单通道编码
                        returncode, stdout, stderr = await run_ffmpeg_command(config['cmd'], timeout=60)
                    
                    if returncode != 0:
                        error_msg = stderr.decode('utf-8', errors='ignore') if stderr else 'Unknown error'
                        logger.warning(f'❌ Config {i+1} failed: {error_msg[:200]}...')
                        last_error = Exception(f'FFmpeg failed: {error_msg}')
                        continue
                    
                    # 使用独立的WebM验证函数
                    is_valid, validation_result = await self.validate_webm(
                        output_file, 
                        max_size=256 * 1024,
                        expected_width=512,
                        expected_height=512
                    )

                    if is_valid:
                        logger.info(f'✅ SUCCESS! WebM conversion with {config["name"]}!')
                        logger.info(f'   📦 Size: {validation_result["file_size"]} bytes')
                        logger.info(f'   🎬 Frames: {validation_result["frame_count"]}')
                        logger.info(f'   ⏱️  Duration: {validation_result["duration"]:.2f}s')
                        logger.info(f'   🎥 Codec: {validation_result["codec_name"]}')
                        
                        # 清理临时文件
                        if isinstance(input_file, (bytes, BytesIO)) and os.path.exists(temp_input):
                            os.unlink(temp_input)
                        
                        return output_file
                    else:
                        logger.warning(f'❌ Validation failed for config {i+1}:')
                        for error in validation_result['errors']:
                            logger.warning(f'   - {error}')
                        
                        # 删除无效文件
                        if os.path.exists(output_file):
                            os.unlink(output_file)
                            
                except TimeoutError as e:
                    logger.error(f'Config {i+1} timed out: {e}')
                    last_error = e
                    continue
                    
                except Exception as e:
                    logger.warning(f'Config {i+1} failed: {e}')
                    last_error = e
                    continue

            # 清理临时文件
            if isinstance(input_file, (bytes, BytesIO)) and os.path.exists(temp_input):
                os.unlink(temp_input)
            
            # 所有配置都失败了
            if last_error:
                raise Exception(f'All conversion attempts failed. Last error: {last_error}')
            else:
                raise Exception('Failed to convert GIF to WebM: No suitable configuration found')
            
        except Exception as err:
            logger.error(f'Error during GIF to WebM conversion: {err}')
            raise err

    async def analyze_gif(self, file_path: str) -> Dict[str, Any]:
        """
        分析GIF文件的详细信息
        
        Args:
            file_path: GIF文件路径
            
        Returns:
            包含GIF信息的字典
        """
        try:
            import ffmpeg
            
            probe = ffmpeg.probe(file_path)
            video_stream = next(s for s in probe['streams'] if s['codec_type'] == 'video')
            
            # 获取原始参数
            original_fps = None
            if 'r_frame_rate' in video_stream:
                fps_parts = video_stream['r_frame_rate'].split('/')
                if len(fps_parts) == 2 and int(fps_parts[1]) != 0:
                    original_fps = float(fps_parts[0]) / float(fps_parts[1])
            
            duration = float(video_stream.get('duration', 0))
            width = int(video_stream.get('width', 0))
            height = int(video_stream.get('height', 0))
            
            # 获取帧数
            nb_frames = video_stream.get('nb_frames')
            if nb_frames and str(nb_frames).isdigit():
                frame_count = int(nb_frames)
            else:
                # 从 tags 中获取
                tags = video_stream.get('tags', {})
                if 'NUMBER_OF_FRAMES' in tags:
                    frame_count = int(tags['NUMBER_OF_FRAMES'])
                elif duration > 0 and original_fps:
                    # 估算帧数
                    frame_count = max(1, int(duration * original_fps))
                else:
                    frame_count = 1
            
            result = {
                'fps': original_fps,
                'duration': duration,
                'width': width,
                'height': height,
                'frame_count': frame_count,
                'codec_name': video_stream.get('codec_name', 'unknown'),
                'is_animated': frame_count > 1 and duration > 0.1,
                'file_path': file_path
            }
            
            # logger.info(f'📊 GIF Analysis Results:')
            # logger.info(f'   🎬 FPS: {original_fps}')
            # logger.info(f'   ⏱️  Duration: {duration}s')
            # logger.info(f'   📏 Size: {width}x{height}')
            # logger.info(f'   🖼️  Frames: {frame_count}')
            # logger.info(f'   🎭 Is Animated: {result["is_animated"]}')
            
            return result
            
        except Exception as e:
            logger.warning(f'Could not analyze GIF: {e}')
            return {
                'fps': 15,
                'duration': 2.0,
                'width': 0,
                'height': 0,
                'frame_count': 1,
                'codec_name': 'unknown',
                'is_animated': False,
                'file_path': file_path,
                'error': str(e)
            }

    async def validate_webm(self, file_path: str, max_size: int = 256 * 1024, 
                        expected_width: int = 512, expected_height: int = 512) -> Tuple[bool, Dict[str, Any]]:
        """
        验证WebM文件是否符合要求
        
        Args:
            file_path: WebM文件路径
            max_size: 最大文件大小（字节）
            expected_width: 期望的视频宽度
            expected_height: 期望的视频高度
            
        Returns:
            (is_valid, analysis_result) 元组
        """
        analysis_result = {
            'file_path': file_path,
            'file_exists': False,
            'file_size': 0,
            'width': 0,
            'height': 0,
            'codec_name': 'unknown',
            'duration': 0.0,
            'frame_count': 0,
            'fps': 0.0,
            'is_animated': False,
            'size_valid': False,
            'dimensions_valid': False,
            'codec_valid': False,
            'animation_valid': False,
            'overall_valid': False,
            'errors': []
        }
        
        try:
            # 检查文件是否存在
            if not os.path.exists(file_path):
                analysis_result['errors'].append('File does not exist')
                return False, analysis_result
            
            analysis_result['file_exists'] = True
            
            # 检查文件大小
            file_size = os.path.getsize(file_path)
            analysis_result['file_size'] = file_size
            analysis_result['size_valid'] = 1000 < file_size <= max_size
            
            if not analysis_result['size_valid']:
                analysis_result['errors'].append(f'Invalid file size: {file_size} bytes (expected: 1000 < size <= {max_size})')
            
            # 使用 ffprobe 分析视频
            probe_cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_format', '-show_streams', file_path
            ]
            
            probe_process = await asyncio.create_subprocess_exec(
                *probe_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            probe_stdout, probe_stderr = await probe_process.communicate()
            
            if probe_process.returncode != 0:
                error_msg = probe_stderr.decode('utf-8', errors='ignore') if probe_stderr else 'Unknown probe error'
                analysis_result['errors'].append(f'FFprobe failed: {error_msg}')
                return False, analysis_result
            
            # 解析 JSON 输出
            probe_data = json.loads(probe_stdout.decode())
            format_info = probe_data.get('format', {})
            streams = probe_data.get('streams', [])
            
            if not streams:
                analysis_result['errors'].append('No video streams found')
                return False, analysis_result
            
            video_stream = streams[0]
            
            # 提取视频信息
            analysis_result['width'] = int(video_stream.get('width', 0))
            analysis_result['height'] = int(video_stream.get('height', 0))
            analysis_result['codec_name'] = video_stream.get('codec_name', 'unknown')
            
            # 获取时长
            duration = float(format_info.get('duration', 0))
            if duration == 0:
                duration = float(video_stream.get('duration', 0))
            analysis_result['duration'] = duration
            
            # 获取帧数
            nb_frames = video_stream.get('nb_frames')
            if nb_frames and str(nb_frames).isdigit():
                frame_count = int(nb_frames)
            else:
                # 从 tags 中获取
                tags = video_stream.get('tags', {})
                if 'NUMBER_OF_FRAMES' in tags:
                    frame_count = int(tags['NUMBER_OF_FRAMES'])
                elif duration > 0:
                    # 估算帧数
                    fps = 15  # 默认
                    if 'r_frame_rate' in video_stream:
                        try:
                            fps_parts = video_stream['r_frame_rate'].split('/')
                            if len(fps_parts) == 2 and int(fps_parts[1]) != 0:
                                fps = float(fps_parts[0]) / float(fps_parts[1])
                        except:
                            pass
                    frame_count = max(2, int(duration * fps))
                    analysis_result['fps'] = fps
                else:
                    frame_count = 0
            
            analysis_result['frame_count'] = frame_count
            
            # 验证各项指标
            analysis_result['dimensions_valid'] = (
                analysis_result['width'] == expected_width and 
                analysis_result['height'] == expected_height
            )
            
            analysis_result['codec_valid'] = analysis_result['codec_name'] in ['vp9', 'libvpx-vp9', 'webp']
            
            analysis_result['is_animated'] = frame_count > 1 and duration > 0.1
            analysis_result['animation_valid'] = analysis_result['is_animated']
            
            # 总体验证
            analysis_result['overall_valid'] = (
                analysis_result['size_valid'] and
                analysis_result['dimensions_valid'] and
                analysis_result['codec_valid'] and
                analysis_result['animation_valid']
            )
            
            # 记录错误
            if not analysis_result['dimensions_valid']:
                analysis_result['errors'].append(f'Invalid dimensions: {analysis_result["width"]}x{analysis_result["height"]} (expected: {expected_width}x{expected_height})')
            
            if not analysis_result['codec_valid']:
                analysis_result['errors'].append(f'Invalid codec: {analysis_result["codec_name"]} (expected: vp9, libvpx-vp9, or webp)')
            
            if not analysis_result['animation_valid']:
                analysis_result['errors'].append(f'Not animated: frames={frame_count}, duration={duration}s')
            
            # 记录分析结果
            logger.info(f'🔍 WebM Validation Results:')
            logger.info(f'   📦 File Size: {file_size} bytes (valid: {analysis_result["size_valid"]})')
            logger.info(f'   📏 Dimensions: {analysis_result["width"]}x{analysis_result["height"]} (valid: {analysis_result["dimensions_valid"]})')
            logger.info(f'   🎥 Codec: {analysis_result["codec_name"]} (valid: {analysis_result["codec_valid"]})')
            logger.info(f'   ⏱️  Duration: {duration:.2f}s')
            logger.info(f'   🖼️  Frames: {frame_count}')
            logger.info(f'   🎬 Is Animated: {analysis_result["is_animated"]} (valid: {analysis_result["animation_valid"]})')
            logger.info(f'   ✅ Overall Valid: {analysis_result["overall_valid"]}')
            
            if analysis_result['errors']:
                logger.warning(f'   ❌ Errors: {"; ".join(analysis_result["errors"])}')
            
            return analysis_result['overall_valid'], analysis_result
            
        except Exception as e:
            error_msg = str(e)
            analysis_result['errors'].append(f'Validation exception: {error_msg}')
            logger.error(f'Error during WebM validation: {error_msg}')
            return False, analysis_result

    async def image_to_webp(self, input_file: Union[str, BytesIO, bytes], output_file: Optional[str] = None, 
                        frame_index: Optional[int] = None, max_size: int = 512, quality: int = 80,
                        static: bool = False) -> str:
        """
        将 图片 转换为 WebP 格式的贴纸
        
        Args:
            input_file: 输入文件路径或字节数据
            output_file: 输出文件路径，如果为 None 则自动生成
            frame_index: 要提取的帧索引，None 表示保留动画，数字表示提取静态帧
            max_size: 最大尺寸，Telegram 贴纸要求 512x512
            quality: WebP 质量 (1-100)
            static: 是否强制转换为静态贴纸
            
        Returns:
            生成的 WebP 文件路径
        """
        try:
            if output_file is None:
                # 根据输入文件类型生成输出文件名
                if isinstance(input_file, str):
                    base_name = os.path.splitext(os.path.basename(input_file))[0]
                    directory = os.path.dirname(input_file)
                    output_file = os.path.join(directory, f"{base_name}.webp") if directory else f"{base_name}.webp"
                else:
                    output_file = "converted_image.webp"

            # 处理输入文件
            if isinstance(input_file, (bytes, BytesIO)):
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    if isinstance(input_file, bytes):
                        temp_file.write(input_file)
                    else:  # BytesIO
                        input_file.seek(0)
                        temp_file.write(input_file.read())
                    temp_input = temp_file.name
            else:
                temp_input = input_file

            # 获取图片格式
            with Image.open(temp_input) as img:
                image_type =  img.format.lower() if img.format else 'gif'
            
            try:
                if image_type == 'gif':
                # 方法1: 优先使用 FFmpeg 转换（支持动画和静态）
                    if frame_index is None and not static:
                        # 保留动画
                        success = await self._gif_to_webp_animated_ffmpeg(temp_input, output_file, max_size, quality)
                    else:
                        # 提取静态帧
                        success = await self._gif_to_webp_static_ffmpeg(temp_input, output_file, frame_index or 0, max_size, quality)
                else:
                    # 其他格式（PNG/JPG/JPEG/WEBP等）转换为静态 WebP
                    success = await self._image_to_webp_ffmpeg(temp_input, output_file, max_size, quality)
                
                if success:
                    logger.info(f'✅ {image_type} to WebP conversion successful (FFmpeg): {output_file}')
                    return output_file
                
            except Exception as e:
                logger.warning(f'FFmpeg conversion failed, trying PIL: {e}')
            
            # 方法2: 回退到 PIL
            if image_type == 'gif':
                if frame_index is None and not static:
                    # 保留动画
                    success = await self._gif_to_webp_animated_pil(temp_input, output_file, max_size, quality)
                else:
                    # 提取静态帧
                    success = await self._gif_to_webp_static_pil(temp_input, output_file, frame_index or 0, max_size, quality)
            else:
                 # 其他格式转换为静态 WebP
                success = await self._image_to_webp_pil(temp_input, output_file, max_size, quality)
            
            if success:
                logger.info(f'✅ {image_type} to WebP conversion successful (PIL): {output_file}')
                return output_file
            else:
                raise Exception('Both FFmpeg and PIL conversion methods failed')

        except Exception as err:
            logger.error(f'Error during {image_type} to WebP conversion: {err}')
            raise err
        
        finally:
            # 清理临时文件
            if isinstance(input_file, bytes) and 'temp_input' in locals() and os.path.exists(temp_input):
                os.unlink(temp_input)

    async def _gif_to_webp_animated_ffmpeg(self, input_file: str, output_file: str, max_size: int, quality: int) -> bool:
        """使用 FFmpeg 转换动画 GIF 为动画 WebP"""
        try:
            # 构建 FFmpeg 命令
            cmd = [
                'ffmpeg', '-i', input_file,
                '-c:v', 'libwebp',
                '-pix_fmt', 'yuva420p',  # 支持透明度
                '-vf', f'scale={max_size}:{max_size}:force_original_aspect_ratio=decrease,pad={max_size}:{max_size}:(ow-iw)/2:(oh-ih)/2:color=0x00000000',
                # '-vf', f'scale={max_size}:{max_size}:force_original_aspect_ratio=decrease,pad={max_size}:{max_size}:(ow-iw)/2:(oh-ih)/2:color=white@0',
                '-lossless', '1',  # 无损压缩
                # '-lossless', '0',
                # '-compression_level', '4',
                '-quality', str(quality),
                '-preset', 'default',
                '-loop', '0',  # 无限循环
                '-an',  # 无音频
                '-f', 'webp',
                '-y', output_file
            ]
            
            # 异步执行 FFmpeg
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60)
            
            if process.returncode == 0 and os.path.exists(output_file):
                return True
            else:
                error_msg = stderr.decode('utf-8', errors='ignore') if stderr else 'Unknown error'
                logger.warning(f'FFmpeg animated conversion failed: {error_msg}')
                return False
                
        except Exception as e:
            logger.warning(f'FFmpeg animated conversion error: {e}')
            return False

    async def _gif_to_webp_static_ffmpeg(self, input_file: str, output_file: str, frame_index: int, max_size: int, quality: int) -> bool:
        """使用 FFmpeg 转换 GIF 的指定帧为静态 WebP"""
        try:
            # 计算时间点（假设每帧 100ms）
            time_point = frame_index * 0.1
            
            cmd = [
                'ffmpeg', '-i', input_file,
                '-ss', str(time_point),
                '-vframes', '1',
                '-c:v', 'libwebp',
                '-pix_fmt', 'yuva420p',  # 支持透明度
                '-vf', f'scale={max_size}:{max_size}:force_original_aspect_ratio=decrease,pad={max_size}:{max_size}:(ow-iw)/2:(oh-ih)/2:color=0x00000000',
                # '-vf', f'scale={max_size}:{max_size}:force_original_aspect_ratio=decrease,pad={max_size}:{max_size}:(ow-iw)/2:(oh-ih)/2:color=white@0',
                '-lossless', '1',  # 无损压缩
                # '-lossless', '0',
                # '-compression_level', '4',
                '-quality', str(quality),
                '-preset', 'default',
                '-f', 'webp',
                '-y', output_file
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
            
            if process.returncode == 0 and os.path.exists(output_file):
                logger.info(f'FFmpeg static GIF to WebP conversion successful (frame {frame_index})')
                return True
            else:
                error_msg = stderr.decode('utf-8', errors='ignore') if stderr else 'Unknown error'
                logger.warning(f'FFmpeg static conversion failed: {error_msg}')
                return False
                
        except Exception as e:
            logger.warning(f'FFmpeg static conversion error: {e}')
            return False

    async def _gif_to_webp_animated_pil(self, input_file: str, output_file: str, max_size: int, quality: int) -> bool:
        """使用 PIL 转换动画 GIF 为动画 WebP"""
        try:
            with Image.open(input_file) as img:
                if not getattr(img, 'is_animated', False):
                    # 不是动画，转换为静态
                    return await self._gif_to_webp_static_pil(input_file, output_file, 0, max_size, quality)
                
                frames = []
                durations = []
                
                for frame_idx in range(img.n_frames):
                    img.seek(frame_idx)
                    frame = img.copy().convert('RGBA')
                    
                    # 调整尺寸
                    frame = await self._resize_image_with_padding(frame, max_size)
                    frames.append(frame)
                    
                    # 获取帧持续时间
                    duration = img.info.get('duration', 100)
                    durations.append(duration)
                
                if frames:
                    # 保存为动画 WebP
                    frames[0].save(
                        output_file,
                        'WEBP',
                        save_all=True,
                        append_images=frames[1:],
                        duration=durations,
                        loop=0,
                        quality=quality,
                        method=6,
                        lossless=False
                    )
                    
                    logger.info('PIL animated GIF to WebP conversion successful')
                    return True
                
            return False
            
        except Exception as e:
            logger.warning(f'PIL animated conversion error: {e}')
            return False

    async def _gif_to_webp_static_pil(self, input_file: str, output_file: str, frame_index: int, max_size: int, quality: int) -> bool:
        """使用 PIL 转换 GIF 的指定帧为静态 WebP"""
        try:
            with Image.open(input_file) as img:
                # 检查是否为动画 GIF
                if getattr(img, 'is_animated', False):
                    # 提取指定帧
                    total_frames = img.n_frames
                    if frame_index >= total_frames:
                        logger.warning(f'Frame index {frame_index} out of range, using last frame')
                        frame_index = total_frames - 1
                    
                    img.seek(frame_index)
                    target_frame = img.copy()
                else:
                    # 静态图片，直接使用
                    target_frame = img.copy()
                
                # 转换为 RGBA 模式以支持透明度
                if target_frame.mode != 'RGBA':
                    target_frame = target_frame.convert('RGBA')
                
                # 调整尺寸
                target_frame = await self._resize_image_with_padding(target_frame, max_size)
                
                # 保存为静态 WebP
                target_frame.save(
                    output_file,
                    'WEBP',
                    quality=quality,
                    method=6,
                    lossless=False
                )
                
                logger.info(f'PIL static GIF to WebP conversion successful (frame {frame_index})')
                return True
                
        except Exception as e:
            logger.warning(f'PIL static conversion error: {e}')
            return False
        
    async def _image_to_webp_ffmpeg(self, input_file: str, output_file: str, max_size: int, quality: int) -> bool:
        """使用 FFmpeg 转换静态图片为 WebP"""
        try:
            cmd = [
                'ffmpeg', '-i', input_file,
                '-c:v', 'libwebp',
                '-pix_fmt', 'yuva420p',  # 支持透明度
                '-vf', f'scale={max_size}:{max_size}:force_original_aspect_ratio=decrease,pad={max_size}:{max_size}:(ow-iw)/2:(oh-ih)/2:color=0x00000000',
                '-lossless', '0',  # 有损压缩以控制文件大小
                '-compression_level', '4',
                '-quality', str(quality),
                '-preset', 'default',
                '-f', 'webp',
                '-y', output_file
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
            
            if process.returncode == 0 and os.path.exists(output_file):
                logger.info('FFmpeg static image to WebP conversion successful')
                return True
            else:
                error_msg = stderr.decode('utf-8', errors='ignore') if stderr else 'Unknown error'
                logger.warning(f'FFmpeg static image conversion failed: {error_msg}')
                return False
                
        except Exception as e:
            logger.warning(f'FFmpeg static image conversion error: {e}')
            return False

    async def _image_to_webp_pil(self, input_file: str, output_file: str, max_size: int, quality: int) -> bool:
        """使用 PIL 转换静态图片为 WebP"""
        try:
            with Image.open(input_file) as img:
                # 转换为 RGBA 模式以支持透明度
                if img.mode != 'RGBA':
                    # 🔍 特殊处理：保留 PNG 的透明度
                    if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
                        img = img.convert('RGBA')
                    else:
                        # JPG 等不支持透明度的格式
                        img = img.convert('RGB')
                        # 创建 RGBA 图像，白色背景
                        rgba_img = Image.new('RGBA', img.size, (255, 255, 255, 255))
                        rgba_img.paste(img, (0, 0))
                        img = rgba_img
                
                # 调整尺寸
                resized_img = await self._resize_image_with_padding(img, max_size)
                
                # 保存为 WebP
                save_kwargs = {
                    'format': 'WEBP',
                    'quality': quality,
                    'method': 6,
                    'lossless': False
                }
                
                # 🔍 如果图像有透明度，确保保存时保留
                if resized_img.mode == 'RGBA':
                    save_kwargs['save_all'] = True
                
                resized_img.save(output_file, **save_kwargs)
                
                logger.info('PIL static image to WebP conversion successful')
                return True
                
        except Exception as e:
            logger.warning(f'PIL static image conversion error: {e}')
            return False

    async def _resize_image_with_padding(self, image: Image.Image, max_size: int) -> Image.Image:
        """调整图片尺寸并添加透明填充以符合正方形要求"""
        width, height = image.size
        
        # 计算缩放比例，保持宽高比
        if width > height:
            new_width = min(width, max_size)
            new_height = int(height * (new_width / width))
        else:
            new_height = min(height, max_size)
            new_width = int(width * (new_height / height))
        
        # 调整图片尺寸
        if new_width != width or new_height != height:
            image = image.resize((new_width, new_height), Image.Resampling.LANCZOS)
        
        # 如果需要填充为正方形
        if new_width != max_size or new_height != max_size:
            # 创建透明背景的正方形画布
            square_image = Image.new('RGBA', (max_size, max_size), (255, 255, 255, 0))
            
            # 计算居中位置
            x = (max_size - new_width) // 2
            y = (max_size - new_height) // 2
            
            # 粘贴图片到中心
            square_image.paste(image, (x, y), image if image.mode == 'RGBA' else None)
            return square_image
        
        return image

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