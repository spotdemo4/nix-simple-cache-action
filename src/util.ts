export function getTextBetween(
	str: string,
	startDelimiter: string,
	endDelimiter: string,
) {
	const startIndex = str.indexOf(startDelimiter);
	if (startIndex === -1) {
		return ""; // Start delimiter not found
	}

	const contentStartIndex = startIndex + startDelimiter.length;
	const endIndex = str.indexOf(endDelimiter, contentStartIndex);
	if (endIndex === -1) {
		return ""; // End delimiter not found
	}

	return str.substring(contentStartIndex, endIndex);
}
