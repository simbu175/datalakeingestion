cd ../..
docker build -t lif:test -f Lake_Ingestion_Codes/docker/Dockerfile.lif .
aws --profile raja --ca-bundle /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 988994707677.dkr.ecr.us-east-1.amazonaws.com
docker push 988994707677.dkr.ecr.us-east-1.amazonaws.com/databi-gaincredit/lif:test
