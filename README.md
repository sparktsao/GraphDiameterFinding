# GraphDiameterFinding
Graph Diameter Finding Based on Spark+GraphX

The algorithm will
* Separate graph into connected components.
* Set the size of each connected components.
* Search for the longest diameters in each connected graph.

But the current algorithm will have issue when the connected graph has cyclic. I already avoid the self loop edges. But if there is cyclic step>=2, the found diameter will be looped until stop at the size of the connected component.
