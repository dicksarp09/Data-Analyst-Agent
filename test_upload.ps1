$filePath = "C:\Users\Dickson\Desktop\Data Analyst Agent\tests\sample_data.csv"
$uri = "http://127.0.0.1:8000/upload_csv"

Invoke-RestMethod -Uri $uri -Method Post -InFile $filePath -ContentType "text/csv"