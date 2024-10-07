
SELECT 
    album, 
    COUNT(*) AS match_count
FROM 
    spotify
WHERE 
    album IN album
GROUP BY 
    album;