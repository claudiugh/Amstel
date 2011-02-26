package nl.vu.cs.amstel;

public class FormatHelper {

	public static String formatTime(long millis) {
		long seconds = millis / 1000;
		if (seconds < 60) {
			return seconds + "s";
		} else {
			long minutes = seconds / 60;
			seconds = seconds % 60;
			return minutes + "m " + seconds + "s";
		}
	}
	
	public static String formatSize(long bytes) {
		long factor = 1;
		int pos = 0;
		String[] abrv = {"B", "KB", "MB", "GB"};
		for (int i = 0; i < 4; i++) {
			if (bytes / factor == 0) {
				pos = i - 1;
				break;
			}
			factor *= 1024;
		}
		return "" + ((bytes * 1024) / factor) + " " + abrv[pos];
	}
}
