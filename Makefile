redeploy:
	juju remove-application mongodb-k8s --destroy-storage || :
	charmcraft pack
	juju deploy ./mongodb_k8s_ubuntu-20.04-amd64.charm --resource mongodb-image=mongo:4.4 --num-units=1
