Vagrant.configure("2") do |config|
    config.vm.box = "ubuntu/trusty64"
    config.vm.box_url = "https://atlas.hashicorp.com/ubuntu/boxes/trusty64/versions/20151218.0.0/providers/virtualbox.box"

    config.vm.provider :virtualbox do |vb|
        vb.name = "Stock Analysis"
        vb.customize ["modifyvm", :id, "--memory", "1024"]
        vb.customize ["modifyvm", :id, "--cpus", "2"]
    end

    config.vm.network :forwarded_port, guest: 5000, host: 5000
    config.vm.network "private_network", ip: "172.17.8.150"
    config.vm.synced_folder ".", "/vagrant", :nfs => true

    config.vm.provision "shell", inline: <<-SHELL
        curl -o anaconda.sh https://repo.continuum.io/archive/Anaconda2-4.2.0-Linux-x86_64.sh;
        rm -rf /vagrant/anaconda;
        bash anaconda.sh -p /vagrant/anaconda -b
        rm anaconda.sh;

        /vagrant/anaconda/bin/pip install eventlet
    SHELL
end
