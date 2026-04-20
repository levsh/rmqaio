import pytest

from rmqaio import UniqueList


class TestUniqueList:

    def test_init_and_uniqueness(self):
        ul = UniqueList([1, 2, 2, 3, 1])
        assert list(ul) == [1, 2, 3]

    def test_append_no_move(self):
        ul = UniqueList([1, 2, 3])
        ul.append(2)
        assert list(ul) == [1, 2, 3]

    def test_append_new(self):
        ul = UniqueList([1, 2])
        ul.append(3)
        assert list(ul) == [1, 2, 3]

    def test_insert_new(self):
        ul = UniqueList([1, 2, 3])
        ul.insert(1, 4)
        assert list(ul) == [1, 4, 2, 3]

    def test_insert_existing_moves(self):
        ul = UniqueList([1, 2, 3])
        ul.insert(0, 3)
        assert list(ul) == [3, 1, 2]

    def test_setitem_replace_new(self):
        ul = UniqueList([1, 2, 3])
        ul[1] = 4
        assert list(ul) == [1, 4, 3]

    def test_setitem_replace_existing(self):
        ul = UniqueList([1, 2, 3])
        ul[0] = 2
        assert list(ul) == [2, 3]

    def test_delitem(self):
        ul = UniqueList([1, 2, 3])
        del ul[1]
        assert list(ul) == [1, 3]

    def test_remove_existing(self):
        ul = UniqueList([1, 2, 3])
        ul.remove(2)
        assert list(ul) == [1, 3]

    def test_remove_missing(self):
        ul = UniqueList([1, 2, 3])
        with pytest.raises(ValueError):
            ul.remove(4)

    def test_contains(self):
        ul = UniqueList([1, 2, 3])
        assert 2 in ul
        assert 4 not in ul

    def test_getitem_index(self):
        ul = UniqueList([1, 2, 3])
        assert ul[0] == 1
        assert ul[-1] == 3

    def test_getitem_slice(self):
        ul = UniqueList([1, 2, 3, 4])
        assert ul[1:3] == [2, 3]

    def test_setitem_slice_not_supported(self):
        ul = UniqueList([1, 2, 3])
        with pytest.raises(TypeError):
            ul[1:2] = [5]

    def test_iteration(self):
        ul = UniqueList([1, 2, 3])
        assert list(ul) == [1, 2, 3]

    def test_copy(self):
        ul1 = UniqueList([1, 2, 3])
        ul2 = ul1.copy()

        assert ul1 == ul2
        assert ul1 is not ul2

    def test_equality_with_list(self):
        ul = UniqueList([1, 2, 3])
        assert ul == [1, 2, 3]
        assert [1, 2, 3] == ul

    def test_equality_with_unique_list(self):
        ul1 = UniqueList([1, 2, 3])
        ul2 = UniqueList([1, 2, 3])
        assert ul1 == ul2

    def test_equality_different(self):
        ul = UniqueList([1, 2, 3])
        assert ul != [1, 2]

    def test_index_error_get(self):
        ul = UniqueList([1, 2, 3])
        with pytest.raises(IndexError):
            _ = ul[10]

    def test_index_error_set(self):
        ul = UniqueList([1, 2, 3])
        with pytest.raises(IndexError):
            ul[10] = 5

    def test_index_error_del(self):
        ul = UniqueList([1, 2, 3])
        with pytest.raises(IndexError):
            del ul[10]

    def test_insert_out_of_bounds(self):
        ul = UniqueList([1, 2])
        ul.insert(100, 3)
        assert list(ul) == [1, 2, 3]

        ul.insert(-100, 4)
        assert list(ul) == [4, 1, 2, 3]
