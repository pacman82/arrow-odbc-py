# Build manylinux wheel

docker build -t cargodock .
docker run --rm -it -v ${PWD}:/io cargodock bash /io/build_wheel.sh
