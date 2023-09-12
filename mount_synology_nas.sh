sudo mkdir -p /share/nas_volume/
sudo apt-get install nfs-common

sudo echo "10.2.136.186:/volume1/Disk1shared/ /share/nas_volume nfs vers=3 0 0" >> /etc/fstab
sudo mount /share/nas_volume/

sudo df
