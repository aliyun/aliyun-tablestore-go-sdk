package search

// GeoGrid represents an area on the earth, which contains a TopLeft and a BottomRight.
// TopLeft and BottomRight form a grid, so the lat of TopLeft should be greater than the lat of BottomRight.
// The lon of TopLeft should be less than the lat of BottomRight.
type GeoGrid struct {
	TopLeft     GeoPoint
	BottomRight GeoPoint
}
