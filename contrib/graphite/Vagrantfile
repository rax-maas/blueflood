Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.provision :shell, path: "scripts/bootstrap.sh"
  config.vm.network :private_network, ip: "192.168.50.4"
end