# --------------------------------------------------------------------
# Developer convenience targets for College Basketball project
# --------------------------------------------------------------------
#  make install           – create venv & install deps
#  make test              – run pytest
#  make fullrun           – train + predict locally
#  make dashboard         – Streamlit UI
#  make tune              – Optuna search on CPU
#  make docker-build/run  – local container workflow
# --------------------------------------------------------------------
#  Lambda GPU (GH200 + H100) helpers:
#     make lambda-sync        – upload code & data to node
#     make lambda-docker      – run tuner in NVIDIA container
#     make lambda-pull        – fetch checkpoints + Optuna DB
#     make lambda-dashboard   – live Optuna web UI on :8080
# --------------------------------------------------------------------

.DEFAULT_GOAL := help
.PHONY: help venv install test fullrun dashboard lint clean \
        tune docker-build docker-run docker-shell \
        lambda-sync lambda-docker lambda-pull lambda-dashboard

IMAGE     ?= college_basketball:latest
ENV_FILE  ?= .env

# ------------------------------------------------------------------
# Help banner
# ------------------------------------------------------------------
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[32m%-18s\033[0m %s\n", $$1, $$2}'

# ------------------------------------------------------------------
# Virtual-env targets
# ------------------------------------------------------------------
venv: .venv/bin/activate      ## Create venv + install deps

.venv/bin/activate: requirements.txt
	python3.11 -m venv .venv
	. .venv/bin/activate && \
	  pip install --upgrade pip && \
	  pip install -r requirements.txt && \
	  pip install -e .
	touch $@

install: venv                ## Alias for venv creation

test: venv                   ## Run pytest
	. .venv/bin/activate && pytest -q

fullrun: venv                ## Train + predict locally
	. .venv/bin/activate && python -m bball.cli fullrun

dashboard: venv              ## Launch Streamlit dashboard
	. .venv/bin/activate && streamlit run streamlit_app.py

tune: venv                   ## Optuna search on CPU
	. .venv/bin/activate && python -m bball.cli tune --trials 30

lint: venv                   ## Placeholder for black / ruff
	@echo "(no linter configured yet)"

clean:                       ## Remove venv & prune local image
	rm -rf .venv
	docker image rm $(IMAGE) 2>/dev/null || true

# ------------------------------------------------------------------
# Local Docker helpers
# ------------------------------------------------------------------
docker-build:                ## Build local image
	docker build -t $(IMAGE) .

docker-run:                  ## Run pipeline in local container
	docker run --rm \
	  --env-file $(ENV_FILE) \
	  -v $(PWD):/app \
	  $(IMAGE) fullrun

docker-shell:                ## Bash inside the image
	docker run --rm -it \
	  --env-file $(ENV_FILE) \
	  -v $(PWD):/app \
	  --entrypoint bash $(IMAGE)

# ==============================================================
# Lambda GPU (GH200 / H100) helpers
# ==============================================================

-include lambda.ini            # pulls host/key/workdir/trials vars
LHOST   ?= $(host)
LKEY    ?= $(key)
LWDIR   ?= $(workdir)
LTRIALS ?= $(trials)

LAM_IMG ?= college_basketball_lambda:latest   # baked env on the node

SYNC_UP   = bball *.py requirements.txt pyproject.toml \
            Makefile training_data.csv optuna_studies.db ./.env
SYNC_DOWN = checkpoints *.db *.json *.csv

lambda-sync: ## Rsync code & data → Lambda node
	@if [ -z "$(LHOST)" ] ; then echo "!! lambda.ini missing"; exit 1; fi
	rsync -avz --delete -e "ssh -i $(LKEY)" $(SYNC_UP) \
		ubuntu@$(LHOST):$(LWDIR)/

# --------------------------------------------------------------
# 1️⃣  Build / cache Python env once                           #
# --------------------------------------------------------------
lambda-build: ## Build container w/ deps installed on Lambda node
	ssh -i $(LKEY) ubuntu@$(LHOST) '\
	  cd $(LWDIR) && \
	  sudo docker run --name cb_env_build --gpus all -i \
	    -v $(LWDIR):/workspace -w /workspace \
	    nvcr.io/nvidia/pytorch:24.03-py3 \
	    bash -c "pip install --upgrade pip && \
	             pip install -r requirements.txt && \
	             pip install -e ." && \
	  sudo docker commit cb_env_build $(LAM_IMG) && \
	  sudo docker container rm cb_env_build'

# --------------------------------------------------------------
# 2️⃣  Launch tuning job (re-uses pre-built image)             #
# --------------------------------------------------------------
lambda-docker: ## Run Optuna tuner in cached Lambda image
	ssh -i $(LKEY) -t ubuntu@$(LHOST) "\
	  sudo docker run --gpus all --rm --network host -it \
  		--env-file $(LWDIR)/.env \
  		-e BBALL_OPTUNA_FRESH=0 \
 		-v $(LWDIR):/workspace -w /workspace \
  		$(LAM_IMG) \
  		python -m bball.cli tune --trials $(LTRIALS)"


lambda-pull:  ## Fetch checkpoints + Optuna DB back to laptop
	@if [ -z "$(LHOST)" ] ; then echo "!! lambda.ini missing"; exit 1; fi
	rsync -avz -e "ssh -i $(LKEY)" \
		"ubuntu@$(LHOST):$(LWDIR)/{checkpoints,*.db,*.json,*.csv}" . || true



lambda-dashboard: ## Tunnel Optuna dashboard on :8080
	ssh -i $(LKEY) -L 8080:127.0.0.1:8080 ubuntu@$(LHOST) "\
	  sudo docker run --gpus all --rm -it \
	    -v $(LWDIR):/workspace -w /workspace \
	    -p 8080:8080 $(LAM_IMG) \
	    optuna dashboard sqlite:///optuna_studies.db --host 0.0.0.0 --port 8080"