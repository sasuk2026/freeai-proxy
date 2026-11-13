FROM node:22-alpine
WORKDIR /app
COPY package.json server.mjs ./
ENV PORT=10000
EXPOSE 10000
CMD ["node", "server.mjs"]
