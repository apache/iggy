# Convinience commands for iggy
# See https://github.com/casey/just
#
# Usage: just <command>
#
# Commands:

start:
  #!/bin/sh
  docker compose up -d

  iggy --username admin --password admin --tcp-server-address localhost:8090 stream create dev || true
  iggy --username admin --password admin --tcp-server-address localhost:8090 topic create 1 dev 1 none || true
  
  echo "Pushing 200 messages to generate fake data"
  for i in {1..200}; do 
    iggy --username admin --password admin --tcp-server-address localhost:8090 message send 1 1 "Hello my name is John and I'm ${i} years"
    iggy --username admin --password admin --tcp-server-address localhost:8090 message send 1 1 "Tempor enim pariatur ea ipsum esse et veniam enim fugiat laborum esse est. Nisi ex occaecat mollit incididunt aute aute. Commodo sunt exercitation est amet exercitation magna enim incididunt esse nisi. Aliquip laborum exercitation pariatur consectetur adipisicing. Non ex commodo nulla laboris. Lorem ad do minim mollit elit eu id cillum incididunt sit. Mollit enim cupidatat irure sint aliquip proident exercitation sint. Nisi exercitation deserunt id irure ad eiusmod consequat. Sint sunt elit anim deserunt nostrud laborum consectetur duis in in elit pariatur quis. Reprehenderit cillum nisi commodo velit ea magna aliqua elit officia consectetur elit proident. Mollit exercitation do aliqua Lorem sit veniam magna. Ad aliquip ea nulla aliquip enim minim nostrud nisi consequat ut id. Ullamco non dolore magna non esse consequat excepteur irure exercitation esse excepteur. Ut aliqua cillum officia nisi sint officia in quis sunt do consequat. Elit aliquip do tempor ipsum excepteur ipsum consectetur deserunt ipsum labore. Fugiat nisi esse amet excepteur dolore velit pariatur deserunt culpa est ad. Esse ullamco elit ex officia irure."
  done

  npm run dev

