IMAGE    := transport-app
PORT     := 8080
# Mount the parent front-end/ directory so ../site resolves correctly inside
ROOT_DIR := $(shell cd .. && pwd)

.PHONY: build rebuild run

build:
	sudo docker build -t $(IMAGE) .

rebuild:
	sudo docker build --no-cache -t $(IMAGE) .

run:
	sudo docker run --rm -it \
		-v "$(ROOT_DIR):/app" \
		-p $(PORT):$(PORT) \
		--name $(IMAGE) \
		$(IMAGE)

ssh1:
	eval "$$(ssh-agent -s)"
	
ssh2:
	ssh-add ~/.ssh/lancaster_uni/id_ed25519

ssh3:
	ssh -T git@github.com

ssh: ssh1 ssh2 ssh3