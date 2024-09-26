import numpy as np
from pydub import AudioSegment
from pydub.audio_segment import AudioSegment as AudioSegmentType
from io import BytesIO

def save_array_to_mp3(array: np.ndarray, sample_rate: int, filename: str) -> None:
    # Ensure the array is normalized between -1 and 1
    normalized_array = np.int16(array * 32767)

    # Create an AudioSegment directly from the numpy array
    audio = AudioSegment(
        normalized_array.tobytes(),
        frame_rate=sample_rate,
        sample_width=2,
        channels=1
    )

    # Export the AudioSegment to MP3
    audio.export(filename, format="mp3")

# # Example usage
# # Generate a sample array (e.g., a sine wave)
# duration = 5  # seconds
# t = np.linspace(0, duration, int(sample_rate * duration), False)
# audio_data = np.sin(2 * np.pi * 440 * t)  # 440 Hz sine wave

# # Save the array to an MP3 file
# save_array_to_mp3(audio_data, sample_rate, "output.mp3")
