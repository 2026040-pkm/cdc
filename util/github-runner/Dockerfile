# 사내 SSL 검사(Somansa) 루트 CA 가 없으면 러너 등록이 Authentication 에서 멈춘다
FROM docker.io/myoung34/github-runner:ubuntu-jammy
COPY somansa-root-ca.crt /usr/local/share/ca-certificates/somansa-root-ca.crt
RUN update-ca-certificates
ENV NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/somansa-root-ca.crt
