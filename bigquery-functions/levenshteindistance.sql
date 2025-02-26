CREATE FUNCTION `sync_metadata.levenshteinDistance` (str1 STRING, str2 STRING)
RETURNS INT64
LANGUAGE js AS """
function levenshteinDistance(str1, str2) {
    const len1 = str1.length;
    const len2 = str2.length;

    const matrix = [];

    // Initialize the matrix
    for (let i = 0; i <= len1; i++) {
        matrix[i] = [i];
    }
    for (let j = 0; j <= len2; j++) {
        matrix[0][j] = j;
    }

    // Fill in the matrix
    for (let i = 1; i <= len1; i++) {
        for (let j = 1; j <= len2; j++) {
            const cost = (str1.charAt(i - 1) === str2.charAt(j - 1)) ? 0 : 1;
            matrix[i][j] = Math.min(
                matrix[i-1][j] + 1,         // deletion
                matrix[i][j-1] + 1,         // insertion
                matrix[i-1][j-1] + cost    // substitution
            );
        }
    }

    // Return the bottom-right cell of the matrix
    return matrix[len1][len2];
}

return levenshteinDistance(str1, str2);
""";
