# Packer Builds

We have Blueflood virtual machines built by Packer.  We currently support both virtualbox and vmware VM's.

The virtual machine will run the latest release jar and also checks out the latest development code in case you want to make some changes and recompile the code.

## Running a Blueflood VM

To download and run a Blueflood VM, simply install vagrant with either virtualbox or vmware, and type:

```
vagrant init blueflood/blueflood
vagrant up
```

If you want to select a specific provider, try something like:

```
vagrant init blueflood/blueflood
vagrant up --provider virtualbox
```

You should then be able to submit the test curl commands from our [[10-Minute-Guide]] to test that Blueflood is running on your system.

## Building new packer images

If you want to build new images yourself, you can make any necessary changes to the files in this ```blueflood-packer``` directory and run:

```
packer build
```

To build
```
packer build  -only=virtualbox-iso -force template.json
```

## Publishing a new packer image

You can publish a new image by using the `packer push` command.

### Set the branch if in development
First, however, if you're testing an image you're working on in a branch, you need to go to:

[`https://atlas.hashicorp.com/blueflood/build-configurations/blueflood/variables`](https://atlas.hashicorp.com/blueflood/build-configurations/blueflood/variables)

And create or set the variable `GIT_BRANCH_FOR_PACKER` to your branch name.  You should reset this to master when you're done.  This only affects the git checkout that occurs in the `blueflood.sh` script.  

> Note that pushing a branch should be taken care of automatically by passing the [`ATLAS_BUILD_GITHUB_BRANCH`](https://atlas.hashicorp.com/help/packer/builds/build-environment#environment-variables) variable into the script.  At the time of this writing, this variable was not working for us.

### Publish the image
Next, execute a push of the blueflood build:

```
packer push -name blueflood/blueflood template.json
```

## Update the new image locally

Once you push a new build, a new image will be available on atlas.  To download and use the new image:

```
vagrant destroy
vagrant box update
vagrant up
```


## Useful Atlas links

Links to various pieces of Blueflood on Atlas:

* Boxes: [https://atlas.hashicorp.com/blueflood/boxes/blueflood](https://atlas.hashicorp.com/blueflood/boxes/blueflood)
* Builds: [https://atlas.hashicorp.com/blueflood/build-configurations/blueflood](https://atlas.hashicorp.com/blueflood/build-configurations/blueflood)
* Artifact: [https://atlas.hashicorp.com/blueflood/artifacts/blueflood](https://atlas.hashicorp.com/blueflood/artifacts/blueflood)